package store_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/quintans/go-scheduler/store/postgres"
	"github.com/stretchr/testify/require"

	"github.com/docker/go-connections/nat"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPgStore(t *testing.T) {
	cfg, tearDown, err := pgSetup()
	require.NoError(t, err)
	defer tearDown()

	db, err := pgConnect(cfg)
	require.NoError(t, err)
	defer db.Close()
	store := postgres.New(db.DB)
	testScheduler(t, store)
}

type PgConfig struct {
	Database string
	Host     string
	Port     int
	Username string
	Password string
}

func pgSetup() (PgConfig, func(), error) {
	dbConfig := PgConfig{
		Database: "eventsourcing",
		Host:     "localhost",
		Port:     5432,
		Username: "postgres",
		Password: "postgres",
	}
	tcpPort := strconv.Itoa(dbConfig.Port)
	natPort := nat.Port(tcpPort)

	req := testcontainers.ContainerRequest{
		Image:        "postgres:12.3",
		ExposedPorts: []string{tcpPort + "/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     dbConfig.Username,
			"POSTGRES_PASSWORD": dbConfig.Password,
			"POSTGRES_DB":       dbConfig.Database,
		},
		WaitingFor: wait.ForListeningPort(natPort),
	}
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return PgConfig{}, nil, fmt.Errorf("failed to initialise container: %w", err)
	}

	tearDown := func() {
		container.Terminate(ctx)
	}

	ip, err := container.Host(ctx)
	if err != nil {
		tearDown()
		return PgConfig{}, nil, fmt.Errorf("failed to get container host: %w", err)
	}
	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		tearDown()
		return PgConfig{}, nil, fmt.Errorf("failed to get container port '%s': %w", natPort, err)
	}

	dbConfig.Host = ip
	dbConfig.Port = port.Int()

	db, err := pgConnect(dbConfig)
	if err != nil {
		tearDown()
		return PgConfig{}, nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	dbSchema(db)

	return dbConfig, tearDown, nil
}

func dbSchema(db *sqlx.DB) {
	db.MustExec(`
	CREATE TABLE IF NOT EXISTS schedules(
		slug VARCHAR (100) PRIMARY KEY,
		kind VARCHAR (100) NOT NULL,
		payload bytea,
		run_at TIMESTAMP NOT NULL,
		version INTEGER NOT NULL,
		retry INTEGER NOT NULL,
		result TEXT,
		locked_until TIMESTAMP
	);
	CREATE INDEX sch_run_at_lock_idx ON schedules (run_at, locked_until);
	`)
}

func pgConnect(dbConfig PgConfig) (*sqlx.DB, error) {
	dburl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", dbConfig.Username, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)

	db, err := sqlx.Open("postgres", dburl)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}
