package firestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	gfs "cloud.google.com/go/firestore"
	"github.com/quintans/go-scheduler/scheduler"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	deleteBatchSize     = 100
	defaultLockDuration = 5 * time.Minute
)

var errOptimisticLocking = errors.New("job was changed by another process")

type Entry struct {
	Slug        string    `firestore:"slug"`
	Payload     []byte    `firestore:"payload,omitempty"`
	When        time.Time `firestore:"run_at"`
	Version     int64     `firestore:"version"`
	Retry       int       `firestore:"retry,omitempty"`
	Result      string    `firestore:"result,omitempty"`
	Trigger     string    `firestore:"trigger_expr,omitempty"`
	Suspended   bool      `firestore:"suspended"`
	LockedUntil time.Time `firestore:"locked_until"`
}

func (e *Entry) Lock(d time.Duration) {
	e.LockedUntil = time.Now().UTC().Add(d)
}

func (e *Entry) Unlock() {
	e.LockedUntil = time.Time{}
}

func (e *Entry) IsUnlocked(t time.Time) bool {
	return e.LockedUntil.Before(t)
}

func toEntry(t *scheduler.Task) *Entry {
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

func fromEntry(e *Entry) *scheduler.Task {
	if e == nil {
		return nil
	}
	return &scheduler.Task{
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

func CollectionPathOption(collectionPath string) StoreOption {
	return func(s *Store) {
		s.collectionPath = collectionPath
	}
}

// Store is a firestore task store.
type Store struct {
	client         *gfs.Client
	lockDuration   time.Duration
	collectionPath string
}

func New(firestoreClient *gfs.Client, options ...StoreOption) *Store {
	ps := &Store{
		client:         firestoreClient,
		lockDuration:   defaultLockDuration,
		collectionPath: "schedules",
	}

	for _, o := range options {
		o(ps)
	}

	return ps
}

func (s *Store) collectionRef() *gfs.CollectionRef {
	return s.client.Collection(s.collectionPath)
}

func (s *Store) docRef(slug string) *gfs.DocumentRef {
	return s.collectionRef().Doc(slug)
}

// CreateSchema makes sure that the store is ready to be used.
// Firestore creates collections implicitly on the first write and automatically indexes
// the single field ("run_at") used to order the queries, so there is nothing to create.
// It is provided so that every store offers the same setup entry point.
func (s *Store) CreateSchema(context.Context) error {
	return nil
}

func (s *Store) Create(ctx context.Context, job *scheduler.Task) error {
	entry := toEntry(job)
	_, err := s.docRef(job.Slug).Create(ctx, entry)
	if status.Code(err) == codes.AlreadyExists {
		return scheduler.ErrTaskAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("failed to schedule '%s': %w", job.Slug, err)
	}

	return nil
}

func (s *Store) NextRun(ctx context.Context) (*scheduler.Task, error) {
	now := time.Now().UTC()

	// ideally we would like to retrieve documents where "locked_until < now" and order ascending by "run_at"
	// but firestore requires that the first order field must be the same as the inequality
	// see: https://firebase.google.com/docs/firestore/query-data/order-limit-data#limitations
	// so we have to adopt another strategy. Get all documents ordered ascending by "run_at" and return the first where "locked_until < now"
	// this is a good compromise since the expected locked records should be low

	iter := s.collectionRef().
		OrderBy("run_at", gfs.Asc).
		Documents(ctx)

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			return nil, scheduler.ErrTaskNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("failed selecting next run: %w", err)
		}
		entry := &Entry{}
		err = doc.DataTo(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to convert doc to entry on next run: %w", err)
		}
		if !entry.Suspended && entry.IsUnlocked(now) {
			return fromEntry(entry), nil
		}
	}
}

func (s *Store) Lock(ctx context.Context, job *scheduler.Task) (*scheduler.Task, error) {
	e, err := s.update(ctx, job.Slug, job.Version, func(old *Entry) (*Entry, error) {
		if old.Suspended {
			return nil, scheduler.ErrTaskNotLocked
		}
		e := toEntry(job)
		// the trigger expression is owned by the store and may have been edited there
		e.Trigger = old.Trigger
		e.Suspended = old.Suspended
		e.Lock(s.lockDuration)
		return e, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to lock '%s': %w", job.Slug, err)
	}
	return fromEntry(e), nil
}

func (s *Store) Reschedule(ctx context.Context, job *scheduler.Task) error {
	_, err := s.update(ctx, job.Slug, job.Version, func(old *Entry) (*Entry, error) {
		e := toEntry(job)
		// the trigger expression and the suspension are owned by the store and may have changed while the job was running
		e.Trigger = old.Trigger
		e.Suspended = old.Suspended
		e.Unlock()
		return e, nil
	})
	if err != nil {
		return fmt.Errorf("failed to reschedule %s: %w", job.Slug, err)
	}
	return nil
}

func (s *Store) Suspend(ctx context.Context, slug string) error {
	_, err := s.modify(ctx, slug, func(e *Entry) (*Entry, error) {
		e.Suspended = true
		return e, nil
	})
	if err != nil {
		return fmt.Errorf("failed to suspend '%s': %w", slug, err)
	}
	return nil
}

func (s *Store) Resume(ctx context.Context, slug string, when time.Time) error {
	_, err := s.modify(ctx, slug, func(e *Entry) (*Entry, error) {
		e.Suspended = false
		e.When = when.UTC()
		return e, nil
	})
	if err != nil {
		return fmt.Errorf("failed to resume '%s': %w", slug, err)
	}
	return nil
}

func (s *Store) GetSlugs(ctx context.Context) ([]string, error) {
	var slugs []string
	iter := s.collectionRef().Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get slugs: %w", err)
		}
		entry := &Entry{}
		err = doc.DataTo(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to convert doc to entry on getting slugs: %w", err)
		}
		slugs = append(slugs, entry.Slug)
	}
	return slugs, nil
}

func (s *Store) Get(ctx context.Context, slug string) (*scheduler.Task, error) {
	doc, err := s.docRef(slug).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, scheduler.ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get '%s': %w", slug, err)
	}
	entry := &Entry{}
	err = doc.DataTo(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to convert doc to entry on get: %w", err)
	}

	return fromEntry(entry), nil
}

func (s *Store) Delete(ctx context.Context, slug string) error {
	_, err := s.docRef(slug).Delete(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete '%s': %w", slug, err)
	}
	return nil
}

func (s *Store) Clear(ctx context.Context) error {
	for {
		// Get a batch of documents
		iter := s.collectionRef().Limit(deleteBatchSize).Documents(ctx)
		numDeleted := 0

		// Iterate through the documents, adding
		// a delete operation for each one to a
		// WriteBatch.
		batch := s.client.Batch()
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to iterate on batch delete: %w", err)
			}

			batch.Delete(doc.Ref)
			numDeleted++
		}

		// If there are no documents to delete,
		// the process is over.
		if numDeleted == 0 {
			return nil
		}

		_, err := batch.Commit(ctx)
		if err != nil {
			return fmt.Errorf("failed to batch delete: %w", err)
		}
	}
}

// update modifies the entry in a transaction, failing if its version is not the expected one, and bumps the version.
func (s *Store) update(ctx context.Context, slug string, version int64, updateFn func(*Entry) (*Entry, error)) (*Entry, error) {
	return s.modify(ctx, slug, func(old *Entry) (*Entry, error) {
		if old.Version != version {
			return nil, errOptimisticLocking
		}
		e, err := updateFn(old)
		if err != nil {
			return nil, err
		}
		e.Version++
		return e, nil
	})
}

// modify modifies the entry in a transaction. The version is left untouched, so a job being run
// (locked with a given version) can still be rescheduled afterwards.
func (s *Store) modify(ctx context.Context, slug string, updateFn func(*Entry) (*Entry, error)) (*Entry, error) {
	ref := s.docRef(slug)
	oldEntry := &Entry{}
	err := s.client.RunTransaction(ctx, func(_ context.Context, tx *gfs.Transaction) error {
		doc, err := tx.Get(ref)
		if status.Code(err) == codes.NotFound {
			return scheduler.ErrTaskNotFound
		}
		if err != nil {
			return err
		}
		err = doc.DataTo(oldEntry)
		if err != nil {
			return err
		}
		oldEntry, err = updateFn(oldEntry)
		if err != nil {
			return err
		}
		return tx.Set(ref, oldEntry)
	})
	if err != nil {
		return nil, err
	}
	return oldEntry, nil
}
