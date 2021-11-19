package store_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	gfs "cloud.google.com/go/firestore"
	"github.com/docker/go-connections/nat"
	_ "github.com/lib/pq"
	"github.com/quintans/go-scheduler/store/firestore"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestFireStore(t *testing.T) {
	cfg, tearDown, err := fireSetup()
	require.NoError(t, err)
	defer tearDown()

	db, err := fireConnect(cfg)
	require.NoError(t, err)
	defer db.Close()
	store := firestore.New(db)
	t.Run("firestore", func(t *testing.T) {
		testScheduler(t, store)
	})
}

type FireConfig struct {
	FirestoreProjectID string
	Host               string
	Port               int
}

func fireSetup() (FireConfig, func(), error) {
	dbConfig := FireConfig{
		FirestoreProjectID: "dummy-project-id",
		Host:               "localhost",
		Port:               8200,
	}
	tcpPort := strconv.Itoa(dbConfig.Port)
	natPort := nat.Port(tcpPort)

	req := testcontainers.ContainerRequest{
		Image:        "mtlynch/firestore-emulator",
		ExposedPorts: []string{tcpPort + "/tcp"},
		Env: map[string]string{
			"FIRESTORE_PROJECT_ID": dbConfig.FirestoreProjectID,
			"PORT":                 strconv.Itoa(dbConfig.Port),
		},
		WaitingFor: wait.ForListeningPort(natPort),
	}
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return FireConfig{}, nil, fmt.Errorf("failed to initialise container: %w", err)
	}

	tearDown := func() {
		container.Terminate(ctx)
	}

	ip, err := container.Host(ctx)
	if err != nil {
		tearDown()
		return FireConfig{}, nil, fmt.Errorf("failed to get container host: %w", err)
	}
	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		tearDown()
		return FireConfig{}, nil, fmt.Errorf("failed to get container port '%s': %w", natPort, err)
	}

	dbConfig.Host = ip
	dbConfig.Port = port.Int()

	envVal := fmt.Sprintf("%s:%d", dbConfig.Host, dbConfig.Port)
	err = os.Setenv("FIRESTORE_EMULATOR_HOST", envVal)
	if err != nil {
		return FireConfig{}, nil, fmt.Errorf("failed to set env var FIRESTORE_EMULATOR_HOST to '%s': %w", envVal, err)
	}

	return dbConfig, tearDown, nil
}

func fireConnect(dbConfig FireConfig) (*gfs.Client, error) {
	return gfs.NewClient(context.Background(), dbConfig.FirestoreProjectID)
}
