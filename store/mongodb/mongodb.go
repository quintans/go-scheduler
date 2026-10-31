package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/quintans/go-scheduler/scheduler"
)

const (
	defaultCollection   = "schedules"
	defaultLockDuration = 5 * time.Minute
)

// Entry is the document persisted in the MongoDB collection.
// The task slug is used as the document id, which makes it unique.
type Entry struct {
	Slug        string     `bson:"_id"`
	Payload     []byte     `bson:"payload,omitempty"`
	When        time.Time  `bson:"run_at"`
	Version     int64      `bson:"version"`
	Retry       int        `bson:"retry"`
	Result      string     `bson:"result,omitempty"`
	Trigger     string     `bson:"trigger_expr,omitempty"`
	Suspended   bool       `bson:"suspended"`
	LockedUntil *time.Time `bson:"locked_until"`
}

func toEntry(t *scheduler.StoreTask) *Entry {
	return &Entry{
		Slug:      t.Slug,
		Payload:   t.Payload,
		When:      t.When.UTC(),
		Version:   t.Version,
		Retry:     t.Retry,
		Result:    t.Result,
		Trigger:   t.Trigger,
		Suspended: t.Suspended,
	}
}

func fromEntry(e *Entry) *scheduler.StoreTask {
	if e == nil {
		return nil
	}
	return &scheduler.StoreTask{
		Slug:      e.Slug,
		Payload:   e.Payload,
		When:      e.When.UTC(),
		Version:   e.Version,
		Retry:     e.Retry,
		Result:    e.Result,
		Trigger:   e.Trigger,
		Suspended: e.Suspended,
	}
}

type StoreOption func(*Store)

// CollectionOption sets the name of the collection where the tasks are stored.
// Defaults to "schedules".
func CollectionOption(collectionName string) StoreOption {
	return func(s *Store) {
		s.collectionName = collectionName
	}
}

// LockDurationOption sets for how long a task stays locked after being acquired.
// After this period the task is considered abandoned and can be locked by another process.
// Defaults to 5 minutes.
func LockDurationOption(d time.Duration) StoreOption {
	return func(s *Store) {
		s.lockDuration = d
	}
}

// Store is a MongoDB task store.
//
// Optimistic locking is done with a version field and the conditional atomic updates
// provided by MongoDB, so no replica set (transactions) is required.
type Store struct {
	db             *mongo.Database
	lockDuration   time.Duration
	collectionName string
}

func New(db *mongo.Database, options ...StoreOption) *Store {
	s := &Store{
		db:             db,
		lockDuration:   defaultLockDuration,
		collectionName: defaultCollection,
	}

	for _, o := range options {
		o(s)
	}

	return s
}

func (s *Store) collection() *mongo.Collection {
	return s.db.Collection(s.collectionName)
}

// CreateIndexes creates the index used by NextRun to find the next task to run.
// It is safe to call it multiple times.
func (s *Store) CreateIndexes(ctx context.Context) error {
	_, err := s.collection().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "run_at", Value: 1},
			{Key: "locked_until", Value: 1},
		},
		Options: options.Index().SetName("sch_run_at_lock_idx"),
	})
	if err != nil {
		return fmt.Errorf("failed to create indexes on '%s': %w", s.collectionName, err)
	}
	return nil
}

func (s *Store) Create(ctx context.Context, task *scheduler.StoreTask) error {
	entry := toEntry(task)
	_, err := s.collection().InsertOne(ctx, entry)
	if err == nil {
		return nil
	}

	if mongo.IsDuplicateKeyError(err) {
		return scheduler.ErrJobAlreadyExists
	}

	return fmt.Errorf("failed to schedule '%s': %w", task.Slug, err)
}

// NextRun returns the next available run
func (s *Store) NextRun(ctx context.Context) (*scheduler.StoreTask, error) {
	now := time.Now().UTC()
	entry := &Entry{}
	err := s.collection().FindOne(
		ctx,
		runnableFilter(now),
		options.FindOne().SetSort(bson.D{{Key: "run_at", Value: 1}}),
	).Decode(entry)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, scheduler.ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed selecting next run: %w", err)
	}

	return fromEntry(entry), nil
}

func (s *Store) Lock(ctx context.Context, task *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	lockUntil := time.Now().UTC().Add(s.lockDuration)
	entry := &Entry{}
	err := s.collection().FindOneAndUpdate(
		ctx,
		bson.D{{Key: "_id", Value: task.Slug}, {Key: "version", Value: task.Version}, notSuspended},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "locked_until", Value: lockUntil}}},
			{Key: "$inc", Value: bson.D{{Key: "version", Value: 1}}},
		},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(entry)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, scheduler.ErrJobNotLocked
	}
	if err != nil {
		return nil, fmt.Errorf("failed to lock '%s': %w", task.Slug, err)
	}

	return fromEntry(entry), nil
}

func (s *Store) Reschedule(ctx context.Context, task *scheduler.StoreTask) error {
	entry := toEntry(task)
	// the trigger expression is owned by the store and may have been edited there, so it is not updated
	res, err := s.collection().UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: entry.Slug}, {Key: "version", Value: entry.Version}},
		bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "payload", Value: entry.Payload},
				{Key: "run_at", Value: entry.When},
				{Key: "retry", Value: entry.Retry},
				{Key: "result", Value: entry.Result},
				{Key: "locked_until", Value: nil},
			}},
			{Key: "$inc", Value: bson.D{{Key: "version", Value: 1}}},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to unlock lock '%s': %w", task.Slug, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to unlock lock %+v: no update: %w", entry, scheduler.ErrJobNotFound)
	}

	return nil
}

func (s *Store) Suspend(ctx context.Context, slug string) error {
	return s.updateSuspended(ctx, "suspend", slug, bson.D{{Key: "suspended", Value: true}})
}

func (s *Store) Resume(ctx context.Context, slug string, when time.Time) error {
	return s.updateSuspended(ctx, "resume", slug, bson.D{{Key: "suspended", Value: false}, {Key: "run_at", Value: when.UTC()}})
}

func (s *Store) updateSuspended(ctx context.Context, action string, slug string, set bson.D) error {
	res, err := s.collection().UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: slug}},
		bson.D{{Key: "$set", Value: set}},
	)
	if err != nil {
		return fmt.Errorf("failed to %s '%s': %w", action, slug, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("%s task '%s': %w", action, slug, scheduler.ErrJobNotFound)
	}

	return nil
}

func (s *Store) GetSlugs(ctx context.Context) ([]string, error) {
	cursor, err := s.collection().Find(
		ctx,
		bson.D{},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get slugs: %w", err)
	}
	defer cursor.Close(ctx)

	slugs := []string{}
	for cursor.Next(ctx) {
		var doc struct {
			Slug string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode slug: %w", err)
		}
		slugs = append(slugs, doc.Slug)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate slugs: %w", err)
	}

	return slugs, nil
}

func (s *Store) Get(ctx context.Context, slug string) (*scheduler.StoreTask, error) {
	entry := &Entry{}
	err := s.collection().FindOne(ctx, bson.D{{Key: "_id", Value: slug}}).Decode(entry)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("get task '%s': %w", slug, scheduler.ErrJobNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get task '%s': %w", slug, err)
	}

	return fromEntry(entry), nil
}

func (s *Store) Delete(ctx context.Context, slug string) error {
	res, err := s.collection().DeleteOne(ctx, bson.D{{Key: "_id", Value: slug}})
	if err != nil {
		return fmt.Errorf("failed to delete '%s': %w", slug, err)
	}
	if res.DeletedCount == 0 {
		return fmt.Errorf("delete task '%s': %w", slug, scheduler.ErrJobNotFound)
	}

	return nil
}

func (s *Store) Clear(ctx context.Context) error {
	_, err := s.collection().DeleteMany(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to clear: %w", err)
	}
	return nil
}

// notSuspended matches the tasks that are not suspended. A missing "suspended" field is also matched.
var notSuspended = bson.E{Key: "suspended", Value: bson.D{{Key: "$ne", Value: true}}}

// runnableFilter matches the tasks that are not suspended and are not locked, or whose lock has expired.
// A missing "locked_until" field is also matched by the "null" comparison.
func runnableFilter(now time.Time) bson.D {
	return bson.D{
		notSuspended,
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "locked_until", Value: nil}},
			bson.D{{Key: "locked_until", Value: bson.D{{Key: "$lt", Value: now}}}},
		}},
	}
}
