package store_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/quintans/go-scheduler/store/memory"
)

func TestMemStore(t *testing.T) {
	store := memory.New()
	require.NoError(t, store.CreateSchema(t.Context()))
	t.Run("memory", func(t *testing.T) {
		testScheduler(t, store)
	})
	t.Run("memory suspend", func(t *testing.T) {
		testStoreSuspend(t, store)
	})
}
