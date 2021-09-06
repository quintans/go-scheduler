package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/quintans/go-scheduler/scheduler"
)

const (
	driverName        = "postgres"
	pgUniqueViolation = "23505"
)

type PgEntry struct {
	Slug    string    `db:"slug"`
	When    time.Time `db:"run_at"`
	Version int64     `db:"version"`
	Retry   int       `db:"retry"`
	Result  string    `db:"result"`
}

func toPgEntry(t *scheduler.StoreTask) *PgEntry {
	return &PgEntry{
		Slug:    t.Slug,
		When:    time.Unix(0, t.When).UTC(),
		Version: t.Version,
		Retry:   t.Retry,
		Result:  t.Result,
	}
}

func fromPgEntry(t *PgEntry) *scheduler.StoreTask {
	return &scheduler.StoreTask{
		Slug:    t.Slug,
		When:    t.When.UnixNano(),
		Version: t.Version,
		Retry:   t.Retry,
		Result:  t.Result,
	}
}

// PgStore is a PostgreSQL task store.
type PgStore struct {
	db           *sqlx.DB
	lockDuration time.Duration
}

func NewPgStore(db *sql.DB) *PgStore {
	return &PgStore{
		db:           sqlx.NewDb(db, driverName),
		lockDuration: 5 * time.Minute,
	}
}

func (s *PgStore) Create(ctx context.Context, task *scheduler.StoreTask) error {
	entry := toPgEntry(task)
	_, err := s.db.NamedExecContext(ctx, "INSERT INTO schedules (slug, run_at, version, retry, result, locked_until) VALUES (:slug, :run_at, :version, :retry, :result, NULL)", entry)
	if err == nil {
		return nil
	}

	if isPgDup(err) {
		return scheduler.ErrJobAlreadyExists
	}

	return fmt.Errorf("failed to schedule task: %w", err)
}

// NextRun returns the next available run
func (s *PgStore) NextRun(ctx context.Context) (int64, error) {
	var runAt time.Time
	now := time.Now().UTC()
	err := s.db.GetContext(ctx, &runAt, "SELECT run_at FROM schedules WHERE locked_until IS NULL OR locked_until < $1 ORDER BY run_at ASC LIMIT 1", now)
	if err == nil {
		return runAt.UnixNano(), nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return 0, scheduler.ErrJobNotFound
	}

	return 0, fmt.Errorf("failed selecting next run: %w", err)
}

func (s *PgStore) Lock(ctx context.Context) (*scheduler.StoreTask, error) {
	var entry *PgEntry
	err := s.withTx(ctx, func(c context.Context, t *sqlx.Tx) error {
		var err error
		entry, err = s.lock(c, t)
		return err
	})
	if err != nil {
		return nil, err
	}
	return fromPgEntry(entry), nil
}

func (s *PgStore) lock(ctx context.Context, t *sqlx.Tx) (*PgEntry, error) {
	now := time.Now().UTC()
	lockUntil := now.Add(s.lockDuration)
	res, err := t.QueryxContext(ctx,
		`UPDATE schedules SET locked_until = $1, version = version + 1
		WHERE slug = (
			SELECT slug
			FROM schedules
			WHERE run_at < $2 AND locked_until IS NULL OR locked_until < $3
			ORDER BY run_at ASC 
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING slug, run_at, version, retry, result`,
		lockUntil, now, now)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, scheduler.ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to lock: %w", err)
	}
	defer res.Close()

	if !res.Next() {
		return nil, scheduler.ErrJobNotFound
	}
	entry := &PgEntry{}
	err = res.StructScan(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to scan locked entry: %w", err)
	}

	return entry, nil
}

func (s *PgStore) Release(ctx context.Context, task *scheduler.StoreTask) error {
	entry := toPgEntry(task)
	res, err := s.db.NamedExecContext(ctx,
		`UPDATE schedules
		SET run_at = :run_at, version = version + 1, retry = :retry, result = :result, locked_until = NULL
		WHERE slug = :slug AND version = :version`,
		entry)
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("failed to release lock %+v: no update: %w", entry, scheduler.ErrJobNotFound)
	}

	return nil
}

func (s *PgStore) GetSlugs(ctx context.Context) ([]string, error) {
	slugs := []string{}
	err := s.db.SelectContext(ctx, &slugs, "SELECT slug FROM schedules")
	if err == nil {
		return slugs, nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get slugs: %w", scheduler.ErrJobNotFound)
	}

	return nil, fmt.Errorf("failed to get slugs: %w", err)
}

func (s *PgStore) Get(ctx context.Context, slug string) (*scheduler.StoreTask, error) {
	entry := &PgEntry{}
	err := s.db.GetContext(ctx, entry, "SELECT slug, run_at, version, retry, result FROM schedules WHERE slug = $1", slug)
	if err == nil {
		return fromPgEntry(entry), nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get task '%s': %w", slug, scheduler.ErrJobNotFound)
	}

	return nil, fmt.Errorf("get task '%s': %w", slug, err)
}

func (s *PgStore) Delete(ctx context.Context, slug string) error {
	res, err := s.db.ExecContext(ctx, "DELETE FROM schedules WHERE slug = $1", slug)
	if err != nil {
		return fmt.Errorf("failed to delete '%s': %w", slug, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows when deleting '%s': %w", slug, err)
	}
	if affected == 0 {
		return fmt.Errorf("delete task '%s': %w", slug, scheduler.ErrJobNotFound)
	}

	return nil
}

func (s *PgStore) Clear(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM schedules")
	if err != nil {
		return fmt.Errorf("failed to clear: %w", err)
	}
	return nil
}

func isPgDup(err error) bool {
	pgerr, ok := err.(*pq.Error)
	return ok && pgerr.Code == pgUniqueViolation
}

func (r *PgStore) withTx(ctx context.Context, fn func(context.Context, *sqlx.Tx) error) (err error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	err = fn(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}
