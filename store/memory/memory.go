package memory

import (
	"container/heap"
	"context"
	"fmt"
	"sync"

	"github.com/quintans/go-scheduler/scheduler"
)

// MemStore simulates a remote storage.
// The remote storage is represented by queue that will hold serializable data
type MemStore struct {
	mu     sync.Mutex
	queue  *PriorityQueue
	locked map[string]*MemEntry
}

func New() *MemStore {
	return &MemStore{
		queue:  &PriorityQueue{},
		locked: map[string]*MemEntry{},
	}
}

func (s *MemStore) Create(_ context.Context, task *scheduler.StoreTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	heap.Push(s.queue, &MemEntry{
		StoreTask: task,
	})

	return nil
}

func (s *MemStore) NextRun(context.Context) (*scheduler.StoreTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queue.Len() == 0 {
		return nil, scheduler.ErrJobNotFound
	}

	ts := s.queue.Head().When

	return &scheduler.StoreTask{
		When: ts,
	}, nil
}

func (s *MemStore) Lock(context.Context, *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queue.Len() == 0 {
		return nil, scheduler.ErrJobNotFound
	}

	// Lock by popping.
	// Since this is in memory, we assume that this is being called when the timer has expired.
	entry := heap.Pop(s.queue).(*MemEntry)
	s.locked[entry.Slug] = entry

	return entry.StoreTask, nil
}

func (s *MemStore) Release(ctx context.Context, task *scheduler.StoreTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.locked[task.Slug]
	if ok {
		delete(s.locked, task.Slug)
		task.Version++
		entry.StoreTask = task
		heap.Push(s.queue, entry)
		return nil
	}
	return scheduler.ErrJobNotFound
}

func (s *MemStore) GetSlugs(context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	slugs := make([]string, 0, s.queue.Len()+len(s.locked))
	for _, entry := range *s.queue {
		slugs = append(slugs, entry.Slug)
	}

	for k := range s.locked {
		slugs = append(slugs, k)
	}

	return slugs, nil
}

func (s *MemStore) Get(ctx context.Context, slug string) (*scheduler.StoreTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range *s.queue {
		if entry.Slug == slug {
			return entry.StoreTask, nil
		}
	}
	for _, entry := range s.locked {
		if entry.Slug == slug {
			return entry.StoreTask, nil
		}
	}

	return nil, fmt.Errorf("get task '%s': %w", slug, scheduler.ErrJobNotFound)
}

func (s *MemStore) Delete(ctx context.Context, slug string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, entry := range *s.queue {
		if entry.Slug == slug {
			s.queue.Remove(i)
			return nil
		}
	}

	_, ok := s.locked[slug]
	if ok {
		delete(s.locked, slug)
		return nil
	}

	return fmt.Errorf("delete task '%s': %w", slug, scheduler.ErrJobNotFound)
}

func (s *MemStore) Clear(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.queue = &PriorityQueue{}
	s.locked = map[string]*MemEntry{}

	return nil
}

type MemEntry struct {
	*scheduler.StoreTask
	index int
}

// PriorityQueue implements the heap.Interface.
type PriorityQueue []*MemEntry

// Len returns the PriorityQueue length.
func (pq PriorityQueue) Len() int { return len(pq) }

// Less is the items less comparator.
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].When.Before(pq[j].When)
}

// Swap exchanges the indexes of the items.
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push implements the heap.Interface.Push.
// Adds x as element Len().
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	entry := x.(*MemEntry)
	entry.index = n
	*pq = append(*pq, entry)
}

// Pop implements the heap.Interface.Pop.
// Removes and returns element Len() - 1.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	entry := old[n-1]
	entry.index = -1 // for safety
	*pq = old[0 : n-1]
	return entry
}

// Head returns the first entry of a PriorityQueue without removing it.
func (pq *PriorityQueue) Head() *MemEntry {
	return (*pq)[0]
}

// Remove removes and returns the element at index i from the PriorityQueue.
func (pq *PriorityQueue) Remove(i int) *MemEntry {
	return heap.Remove(pq, i).(*MemEntry)
}
