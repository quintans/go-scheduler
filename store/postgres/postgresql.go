package postgres

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

const defaultLockDuration = 5 * time.Minute

type Entry struct {
	Slug    string    `db:"slug"`
	Payload []byte    `db:"payload"`
	When    time.Time `db:"run_at"`
	Version int64     `db:"version"`
	Retry   int       `db:"retry"`
	Result  string    `db:"result"`
}

func toEntry(t *scheduler.StoreTask) *Entry {
	return &Entry{
		Slug:    t.Slug,
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
		Payload: e.Payload,
		When:    e.When.UTC(),
		Version: e.Version,
		Retry:   e.Retry,
		Result:  e.Result,
	}
}

type StoreOption func(*Store)

func TableOption(tableName string) StoreOption {
	return func(ps *Store) {
		ps.tableName = tableName
	}
}

// Store is a PostgreSQL task store.
type Store struct {
	db           *sqlx.DB
	lockDuration time.Duration
	tableName    string
}

func New(db *sql.DB, options ...StoreOption) *Store {
	ps := &Store{
		db:           sqlx.NewDb(db, driverName),
		lockDuration: defaultLockDuration,
		tableName:    "schedules",
	}

	for _, o := range options {
		o(ps)
	}

	return ps
}

func (s *Store) Create(ctx context.Context, task *scheduler.StoreTask) error {
	entry := toEntry(task)
	_, err := s.db.NamedExecContext(
		ctx,
		fmt.Sprintf(`INSERT INTO %s (slug, payload, run_at, version, retry, result, locked_until)
		VALUES (:slug, :payload, :run_at, :version, :retry, :result, NULL)`, s.tableName),
		entry,
	)
	if err == nil {
		return nil
	}

	if isDup(err) {
		return scheduler.ErrJobAlreadyExists
	}

	return fmt.Errorf("failed to schedule task: %w", err)
}

// NextRun returns the next available run
func (s *Store) NextRun(ctx context.Context) (*scheduler.StoreTask, error) {
	now := time.Now().UTC()
	entry := &Entry{}
	err := s.db.GetContext(ctx, entry, fmt.Sprintf(`SELECT slug, payload, run_at, version, retry, result FROM %s
	WHERE locked_until IS NULL OR locked_until < $1
	ORDER BY run_at
	ASC LIMIT 1`,
		s.tableName), now)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, scheduler.ErrJobNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("failed selecting next run: %w", err)
	}

	return fromEntry(entry), nil
}

func (s *Store) Lock(ctx context.Context, task *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	var entry *scheduler.StoreTask
	err := s.withTx(ctx, func(c context.Context, t *sqlx.Tx) error {
		var err error
		entry, err = s.lock(c, t, task)
		return err
	})
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *Store) lock(ctx context.Context, t *sqlx.Tx, task *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	now := time.Now().UTC()
	lockUntil := now.Add(s.lockDuration)
	res, err := t.ExecContext(ctx,
		fmt.Sprintf(`UPDATE %s SET locked_until = $1, version = version + 1
		WHERE slug = $2 AND version = $3`, s.tableName),
		lockUntil, task.Slug, task.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to lock: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get affected rows when locking '%s': %w", task.Slug, err)
	}
	if affected == 0 {
		return nil, scheduler.ErrJobNotLocked
	}

	entry := *task // copy
	entry.Version++

	return &entry, nil
}

func (s *Store) Reschedule(ctx context.Context, task *scheduler.StoreTask) error {
	entry := toEntry(task)
	res, err := s.db.NamedExecContext(ctx,
		fmt.Sprintf(`UPDATE %s
		SET payload = :payload, run_at = :run_at, version = version + 1, retry = :retry, result = :result, locked_until = NULL
		WHERE slug = :slug AND version = :version`, s.tableName),
		entry)
	if err != nil {
		return fmt.Errorf("failed to unlock lock: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("failed to unlock lock %+v: no update: %w", entry, scheduler.ErrJobNotFound)
	}

	return nil
}

func (s *Store) GetSlugs(ctx context.Context) ([]string, error) {
	slugs := []string{}
	err := s.db.SelectContext(ctx, &slugs, fmt.Sprintf("SELECT slug FROM %s", s.tableName))
	if err == nil {
		return slugs, nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get slugs: %w", scheduler.ErrJobNotFound)
	}

	return nil, fmt.Errorf("failed to get slugs: %w", err)
}

func (s *Store) Get(ctx context.Context, slug string) (*scheduler.StoreTask, error) {
	entry := &Entry{}
	err := s.db.GetContext(ctx, entry, fmt.Sprintf("SELECT slug, payload, run_at, version, retry, result FROM %s WHERE slug = $1", s.tableName), slug)
	if err == nil {
		return fromEntry(entry), nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get task '%s': %w", slug, scheduler.ErrJobNotFound)
	}

	return nil, fmt.Errorf("get task '%s': %w", slug, err)
}

func (s *Store) Delete(ctx context.Context, slug string) error {
	res, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE slug = $1", s.tableName), slug)
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

func (s *Store) Clear(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", s.tableName))
	if err != nil {
		return fmt.Errorf("failed to clear: %w", err)
	}
	return nil
}

func isDup(err error) bool {
	pgerr, ok := err.(*pq.Error)
	return ok && pgerr.Code == pgUniqueViolation
}

func (s *Store) withTx(ctx context.Context, fn func(context.Context, *sqlx.Tx) error) (err error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	err = fn(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}
