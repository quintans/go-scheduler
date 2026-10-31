package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/quintans/go-scheduler/store/mongodb"
)

func TestMongoStore(t *testing.T) {
	uri, tearDown, err := mongoSetup()
	require.NoError(t, err)
	defer tearDown()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())

	ctx := context.Background()
	require.NoError(t, client.Ping(ctx, nil))

	store := mongodb.New(client.Database("scheduler"))
	require.NoError(t, store.CreateIndexes(ctx))
	t.Run("mongodb", func(t *testing.T) {
		testScheduler(t, store)
	})
	t.Run("mongodb suspend", func(t *testing.T) {
		testStoreSuspend(t, store)
	})
}

func mongoSetup() (string, func(), error) {
	natPort := nat.Port("27017/tcp")

	req := testcontainers.ContainerRequest{
		Image:        "mongo:7",
		ExposedPorts: []string{string(natPort)},
		WaitingFor:   wait.ForListeningPort(natPort),
	}
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to initialise container: %w", err)
	}

	tearDown := func() {
		_ = container.Terminate(ctx)
	}

	ip, err := container.Host(ctx)
	if err != nil {
		tearDown()
		return "", nil, fmt.Errorf("failed to get container host: %w", err)
	}
	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		tearDown()
		return "", nil, fmt.Errorf("failed to get container port '%s': %w", natPort, err)
	}

	return fmt.Sprintf("mongodb://%s:%d", ip, port.Int()), tearDown, nil
}
