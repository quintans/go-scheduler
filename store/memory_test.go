package store_test

import (
	"testing"

	"github.com/quintans/go-scheduler/store/memory"
)

func TestMemStore(t *testing.T) {
	store := memory.New()
	t.Run("memory", func(t *testing.T) {
		testScheduler(t, store)
	})
}
