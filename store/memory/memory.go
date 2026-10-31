package memory

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

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

	for _, entry := range *s.queue {
		if entry.Slug == task.Slug {
			return fmt.Errorf("create task '%s': %w", task.Slug, scheduler.ErrJobAlreadyExists)
		}
	}

	heap.Push(s.queue, &MemEntry{
		StoreTask: task,
	})

	return nil
}

func (s *MemStore) NextRun(context.Context) (*scheduler.StoreTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// suspended entries are kept at the end of the queue, so if the head is suspended there is nothing to run
	if s.queue.Len() == 0 || s.queue.Head().Suspended {
		return nil, scheduler.ErrJobNotFound
	}

	head := s.queue.Head()

	return &scheduler.StoreTask{
		Slug:    head.Slug,
		When:    head.When,
		Version: head.Version,
	}, nil
}

func (s *MemStore) Lock(_ context.Context, task *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queue.Len() == 0 {
		return nil, scheduler.ErrJobNotFound
	}

	// Lock by popping.
	// Since this is in memory, we assume that this is being called when the timer has expired.
	// The head must still be the task found by NextRun: it may have been suspended, resumed or deleted in the meantime.
	head := s.queue.Head()
	if head.Suspended || head.Slug != task.Slug || head.Version != task.Version {
		return nil, scheduler.ErrJobNotLocked
	}
	entry := heap.Pop(s.queue).(*MemEntry)
	s.locked[entry.Slug] = entry

	// hand out a copy, so that changes made to the stored task while it runs (suspend, resume) do not race with the job
	locked := *entry.StoreTask
	return &locked, nil
}

func (s *MemStore) Reschedule(ctx context.Context, task *scheduler.StoreTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.locked[task.Slug]
	if ok {
		delete(s.locked, task.Slug)
		task.Version++
		// the suspension is owned by the store and may have changed while the task was running
		task.Suspended = entry.Suspended
		entry.StoreTask = task
		heap.Push(s.queue, entry)
		return nil
	}
	return scheduler.ErrJobNotFound
}

func (s *MemStore) Suspend(_ context.Context, slug string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, queued := s.find(slug)
	if entry == nil {
		return fmt.Errorf("suspend task '%s': %w", slug, scheduler.ErrJobNotFound)
	}

	entry.Suspended = true
	if queued {
		heap.Fix(s.queue, entry.index)
	}

	return nil
}

func (s *MemStore) Resume(_ context.Context, slug string, when time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, queued := s.find(slug)
	if entry == nil {
		return fmt.Errorf("resume task '%s': %w", slug, scheduler.ErrJobNotFound)
	}

	entry.Suspended = false
	entry.When = when
	if queued {
		heap.Fix(s.queue, entry.index)
	}

	return nil
}

// find returns the entry with the given slug, either queued or locked, and whether it is queued.
// The caller must hold the lock.
func (s *MemStore) find(slug string) (entry *MemEntry, queued bool) {
	for _, entry := range *s.queue {
		if entry.Slug == slug {
			return entry, true
		}
	}
	if entry, ok := s.locked[slug]; ok {
		return entry, false
	}

	return nil, false
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
// Suspended entries go last, so the head of the queue is always the next runnable entry, if any.
func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].Suspended != pq[j].Suspended {
		return !pq[i].Suspended
	}
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
func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	entry := x.(*MemEntry)
	entry.index = n
	*pq = append(*pq, entry)
}

// Pop implements the heap.Interface.Pop.
// Removes and returns element Len() - 1.
func (pq *PriorityQueue) Pop() any {
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
