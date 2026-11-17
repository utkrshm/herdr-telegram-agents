package app

import (
	"context"
	"errors"
	"sync"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

// topicView is a goroutine-safe snapshot of the mapping for readers outside
// the reconciler goroutine (the bridge). The reconciler republishes it after
// every mapping change, so a reader sees at worst the state before the
// current Telegram call. Entries are copies; nothing here aliases the
// mapping.
type topicView struct {
	mu          sync.RWMutex
	entries     map[domain.Key]domain.TopicEntry
	byThread    map[int]domain.Key
	generations map[domain.Key]*topicGeneration
}

type topicGeneration struct {
	thread int
	ctx    context.Context
	cancel context.CancelFunc
}

var errOwnerTopicChanged = errors.New("owner topic mapping changed")

func newTopicView() *topicView {
	return &topicView{entries: map[domain.Key]domain.TopicEntry{}, byThread: map[int]domain.Key{}, generations: map[domain.Key]*topicGeneration{}}
}

// publish replaces the snapshot with the mapping's current content.
func (v *topicView) publish(m *domain.Mapping) {
	entries := make(map[domain.Key]domain.TopicEntry, len(m.Topics))
	byThread := make(map[int]domain.Key, len(m.Topics))
	for _, key := range m.Keys() {
		e, ok := m.TopicFor(key)
		if !ok {
			continue
		}
		entries[key] = *e
		if _, seen := byThread[e.ThreadID]; !seen {
			if k, ok := m.KeyForThread(e.ThreadID); ok {
				byThread[e.ThreadID] = k
			}
		}
	}
	v.mu.Lock()
	for key, gen := range v.generations {
		e, ok := entries[key]
		if !ok || e.ThreadID != gen.thread || byThread[gen.thread] != key || !e.Status.Live() || e.Closed || e.Muted {
			gen.cancel()
			delete(v.generations, key)
		}
	}
	v.entries, v.byThread = entries, byThread
	v.mu.Unlock()
}

// generation pins a key/thread association and is cancelled on remapping.
// Already dispatched network requests cannot be recalled reliably.
func (v *topicView) generation(key domain.Key, thread int) (*topicGeneration, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	e, ok := v.entries[key]
	if !ok || e.ThreadID != thread || v.byThread[thread] != key || !e.Status.Live() || e.Closed || e.Muted {
		return nil, errOwnerTopicChanged
	}
	if gen := v.generations[key]; gen != nil {
		return gen, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	gen := &topicGeneration{thread: thread, ctx: ctx, cancel: cancel}
	v.generations[key] = gen
	return gen, nil
}

func (v *topicView) dispatchGuard(key domain.Key, thread int) domain.DispatchGuard {
	return func(ctx context.Context) (context.Context, context.CancelFunc, error) {
		gen, err := v.generation(key, thread)
		if err != nil {
			return nil, nil, err
		}
		return gen.guard(ctx)
	}
}

func (v *topicView) invalidate(key domain.Key) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if gen := v.generations[key]; gen != nil {
		gen.cancel()
		delete(v.generations, key)
	}
}

func (g *topicGeneration) guard(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if g.ctx.Err() != nil {
		return nil, nil, errOwnerTopicChanged
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.ctx, cancel)
	if g.ctx.Err() != nil {
		stop()
		cancel()
		return nil, nil, errOwnerTopicChanged
	}
	return call, func() { stop(); cancel() }, nil
}

// Entry returns a copy of the entry for key.
func (v *topicView) Entry(key domain.Key) (domain.TopicEntry, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	e, ok := v.entries[key]
	return e, ok
}

// KeyForThread returns the agent behind a topic, newest entry first.
func (v *topicView) KeyForThread(threadID int) (domain.Key, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	k, ok := v.byThread[threadID]
	return k, ok
}

func (v *topicView) ThreadForKey(key domain.Key) (int, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	e, ok := v.entries[key]
	return e.ThreadID, ok && e.ThreadID > 0 && e.Status.Live() && !e.Closed && !e.Muted
}
