package store_test

import (
	"testing"

	"github.com/quintans/go-scheduler/store/memory"
)

func TestMemStore(t *testing.T) {
	store := memory.New()
	testScheduler(t, store)
}
