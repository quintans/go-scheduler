package store_test

import (
	"testing"

	"github.com/quintans/go-scheduler/store"
)

func TestMemStore(t *testing.T) {
	store := store.NewMemStore()
	testScheduler(t, store)
}
