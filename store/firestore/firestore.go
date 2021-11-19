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

const deleteBatchSize = 100

var errOptimisticLocking = errors.New("job was changed by another process")

type Entry struct {
	Slug        string    `firestore:"slug"`
	Kind        string    `firestore:"kind"`
	Payload     []byte    `firestore:"payload,omitempty"`
	When        time.Time `firestore:"run_at"`
	Version     int64     `firestore:"version"`
	Retry       int       `firestore:"retry,omitempty"`
	Result      string    `firestore:"result,omitempty"`
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

func toEntry(t *scheduler.StoreTask) *Entry {
	return &Entry{
		Slug:    t.Slug,
		Kind:    t.Kind,
		Payload: t.Payload,
		When:    t.When.UTC(),
		Version: t.Version,
		Retry:   t.Retry,
		Result:  t.Result,
	}
}

func fromEntry(e *Entry) *scheduler.StoreTask {
	if e == nil {
		return nil
	}
	return &scheduler.StoreTask{
		Slug:    e.Slug,
		Kind:    e.Kind,
		Payload: e.Payload,
		When:    e.When.UTC(),
		Version: e.Version,
		Retry:   e.Retry,
		Result:  e.Result,
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
		lockDuration:   5 * time.Minute,
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

func (s *Store) Create(ctx context.Context, task *scheduler.StoreTask) error {
	entry := toEntry(task)
	_, err := s.docRef(task.Slug).Create(ctx, entry)
	if status.Code(err) == codes.AlreadyExists {
		return scheduler.ErrJobAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("failed to schedule '%s': %w", task.Slug, err)
	}

	return nil
}

func (s *Store) NextRun(ctx context.Context) (*scheduler.StoreTask, error) {
	now := time.Now().UTC()

	// ideally we would like to retrive documents where "locked_until < now" and order ascending by "run_at"
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
			return nil, scheduler.ErrJobNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("failed selecting next run: %w", err)
		}
		entry := &Entry{}
		err = doc.DataTo(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to convert doc to entry on next run: %w", err)
		}
		if entry.IsUnlocked(now) {
			return fromEntry(entry), nil
		}
	}
}

func (s *Store) Lock(ctx context.Context, task *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	e, err := s.update(ctx, task.Slug, task.Version, func(_ *Entry) (*Entry, error) {
		e := toEntry(task)
		e.Lock(s.lockDuration)
		return e, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to lock '%s': %w", task.Slug, err)
	}
	return fromEntry(e), nil
}

func (s *Store) Unlock(ctx context.Context, task *scheduler.StoreTask) error {
	_, err := s.update(ctx, task.Slug, task.Version, func(_ *Entry) (*Entry, error) {
		e := toEntry(task)
		e.Unlock()
		return e, nil
	})
	if err != nil {
		return fmt.Errorf("failed to unlock lock '%s': %w", task.Slug, err)
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

func (s *Store) Get(ctx context.Context, slug string) (*scheduler.StoreTask, error) {
	doc, err := s.docRef(slug).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return nil, scheduler.ErrJobNotFound
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

func (s *Store) update(ctx context.Context, slug string, version int64, updateFn func(*Entry) (*Entry, error)) (*Entry, error) {
	ref := s.docRef(slug)
	oldEntry := &Entry{}
	err := s.client.RunTransaction(ctx, func(_ context.Context, tx *gfs.Transaction) error {
		doc, err := tx.Get(ref)
		if err != nil {
			return err
		}
		if err := doc.DataTo(oldEntry); err != nil {
			return err
		}
		if oldEntry.Version != version {
			return errOptimisticLocking
		}
		oldEntry, err = updateFn(oldEntry)
		if err != nil {
			return err
		}
		oldEntry.Version++
		return tx.Set(ref, oldEntry)
	})
	if err != nil {
		return nil, err
	}
	return oldEntry, nil
}
