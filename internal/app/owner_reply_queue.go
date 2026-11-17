package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

// Admission and quarantine have separate bounds: failures must never consume
// the capacity needed for new replies. Quarantine does not retry messages.
const (
	maxOwnerReplyBytes      = 4 << 20
	ownerReplyCapacity      = 32
	ownerQuarantineCapacity = 32
)

type ownerReply struct {
	key        domain.Key
	hash       string
	revision   uint64
	out        domain.Outgoing
	document   *domain.Document
	generation *topicGeneration
}

type ownerReplyResult struct {
	reply  ownerReply
	lastID int
	err    error
}

func (b *Bridge) enqueueOwnerReply(reply ownerReply) error {
	size := len(reply.out.Text)
	if reply.document != nil {
		size += len(reply.document.Data)
	}
	if size > maxOwnerReplyBytes {
		b.notifyOwnerReplyRejected(reply)
		return errors.New("reply exceeds 4 MiB queue limit; check the agent transcript")
	}
	gen, err := b.out.topics.generation(reply.key, reply.out.ThreadID)
	if err != nil {
		b.notifyOwnerReplyRejected(reply)
		return err
	}
	reply.generation = gen
	select {
	case b.ownerReplySlots <- struct{}{}:
	default:
		b.notifyOwnerReplyRejected(reply)
		return errors.New("owner reply queue full; check the agent transcript")
	}
	select {
	case b.ownerReplies <- reply:
		return nil
	default:
		<-b.ownerReplySlots
		b.notifyOwnerReplyRejected(reply)
		return errors.New("owner reply queue full; check the agent transcript")
	}
}

// Commands already receive their own failure reply. Automatic done posts
// need a separate, bounded best-effort notice when admission fails.
func (b *Bridge) notifyOwnerReplyRejected(reply ownerReply) {
	if reply.hash == "" {
		return
	}
	b.notifyOwnerReplyIssue(fmt.Sprintf("⚠️ Output for topic %d could not be queued. Check the agent transcript and daemon logs.", reply.out.ThreadID))
}

// quarantineOwnerReply retains recent failed source and acknowledgement in
// memory. Eviction is reported, not retried; the transcript may be needed to
// recover the discarded source. Called only on the bridge goroutine.
func (b *Bridge) quarantineOwnerReply(result ownerReplyResult) {
	if len(b.failedOwnerReplies) == ownerQuarantineCapacity {
		old := b.failedOwnerReplies[0]
		copy(b.failedOwnerReplies, b.failedOwnerReplies[1:])
		b.failedOwnerReplies[len(b.failedOwnerReplies)-1] = result
		b.log.Error("owner reply quarantine evicted oldest failed output", slog.Int("thread_id", old.reply.out.ThreadID), slog.Int("last_acknowledged_id", old.lastID), slog.Any("err", old.err))
		b.notifyOwnerReplyIssue(fmt.Sprintf("⚠️ Failed output for topic %d was evicted from memory (last acknowledged message %d). Check the agent transcript and daemon logs; it was not retried.", old.reply.out.ThreadID, old.lastID))
		return
	}
	b.failedOwnerReplies = append(b.failedOwnerReplies, result)
}

func (b *Bridge) notifyOwnerReplyIssue(text string) {
	select {
	case b.ownerNoticeSlot <- struct{}{}:
	default:
		b.log.Error("owner reply issue notice already pending", slog.String("notice", text))
		return
	}
	b.spawn(func(ctx context.Context) {
		defer func() { <-b.ownerNoticeSlot }()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := b.out.tg.Send(ctx, domain.Outgoing{Text: text})
		if err != nil {
			b.log.Error("owner reply rejection notice failed", slog.Any("err", err))
		}
	})
}

func (b *Bridge) runOwnerReplies(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case reply := <-b.ownerReplies:
			var id int
			var err error
			if reply.generation.ctx.Err() != nil {
				err = errOwnerTopicChanged
			} else if reply.document != nil {
				tg := b.out.tg.(interface {
					SendDocumentGuard(context.Context, domain.Document, domain.DispatchGuard) error
				})
				err = tg.SendDocumentGuard(ctx, *reply.document, reply.generation.guard)
			} else {
				tg := b.out.tg.(interface {
					SendGuard(context.Context, domain.Outgoing, domain.DispatchGuard) (int, error)
				})
				id, err = tg.SendGuard(ctx, reply.out, reply.generation.guard)
			}
			result := ownerReplyResult{reply: reply, lastID: id, err: err}
			if submitErr := b.SubmitContext(ctx, result); submitErr != nil {
				<-b.ownerReplySlots
				b.log.Error("owner reply result not accepted", slog.String("key", reply.key.String()), slog.Any("err", submitErr), slog.Any("delivery_err", err), slog.Int("last_acknowledged_id", id))
				return
			}
			if err != nil {
				b.log.Error("owner reply quarantined; delivery continues", slog.String("key", reply.key.String()), slog.Any("err", err), slog.Int("last_acknowledged_id", id))
				// Report in General, never to a potentially reassigned topic.
				noticeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, noticeErr := b.out.tg.Send(noticeCtx, domain.Outgoing{Text: fmt.Sprintf("⚠️ Delivery to topic %d stopped after message %d. Some output may be missing; no automatic retry. Check the agent transcript and daemon logs.", reply.out.ThreadID, id)})
				cancel()
				if noticeErr != nil {
					b.log.Error("owner delivery failure notice failed", slog.Any("err", noticeErr))
				}
			}
		}
	}
}
