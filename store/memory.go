package store

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
	locked map[string]*Item
}

func NewMemStore() *MemStore {
	return &MemStore{
		queue:  &PriorityQueue{},
		locked: map[string]*Item{},
	}
}

func (s *MemStore) Create(_ context.Context, task *scheduler.StoreTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	heap.Push(s.queue, &Item{
		StoreTask: task,
	})

	return nil
}

func (s *MemStore) NextRun(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queue.Len() == 0 {
		return 0, nil
	}

	ts := s.queue.Head().When

	return ts, nil
}

func (s *MemStore) RunAndReschedule(ctx context.Context, fn func(ctx context.Context, task *scheduler.StoreTask) *scheduler.StoreTask) error {
	item := s.lock()
	if item == nil {
		return nil
	}

	task := fn(ctx, item.StoreTask)
	if task == nil {
		s.Delete(ctx, item.Slug)
		return nil
	}
	task.Version++
	item.StoreTask = task
	s.release(item)

	return nil
}

func (s *MemStore) lock() *Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queue.Len() == 0 {
		return nil
	}

	// Lock by popping.
	// Since this is in memory, we assume that this is being called when the timer has expired.
	item := heap.Pop(s.queue).(*Item)
	s.locked[item.Slug] = item

	return item
}

func (s *MemStore) release(item *Item) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.locked, item.Slug)
	heap.Push(s.queue, item)
}

func (s *MemStore) GetSlugs(context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	slugs := make([]string, 0, s.queue.Len()+len(s.locked))
	for _, item := range *s.queue {
		slugs = append(slugs, item.Slug)
	}

	for k := range s.locked {
		slugs = append(slugs, k)
	}

	return slugs, nil
}

func (s *MemStore) Get(ctx context.Context, slug string) (*scheduler.StoreTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, item := range *s.queue {
		if item.Slug == slug {
			return item.StoreTask, nil
		}
	}
	for _, item := range s.locked {
		if item.Slug == slug {
			return item.StoreTask, nil
		}
	}

	return nil, fmt.Errorf("get task '%s': %w", slug, scheduler.ErrJobNotFound)
}

func (s *MemStore) Delete(ctx context.Context, slug string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, item := range *s.queue {
		if item.Slug == slug {
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
	s.locked = map[string]*Item{}

	return nil
}

type Item struct {
	*scheduler.StoreTask
	index int
}

// PriorityQueue implements the heap.Interface.
type PriorityQueue []*Item

// Len returns the PriorityQueue length.
func (pq PriorityQueue) Len() int { return len(pq) }

// Less is the items less comparator.
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].When < pq[j].When
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
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

// Pop implements the heap.Interface.Pop.
// Removes and returns element Len() - 1.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Head returns the first item of a PriorityQueue without removing it.
func (pq *PriorityQueue) Head() *Item {
	return (*pq)[0]
}

// Remove removes and returns the element at index i from the PriorityQueue.
func (pq *PriorityQueue) Remove(i int) *Item {
	return heap.Remove(pq, i).(*Item)
}
