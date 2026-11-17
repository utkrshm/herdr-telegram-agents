package app

import (
	"context"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

// Telegram's hosted Bot API accepts documents up to 50 MB, not 1 GB.
const telegramDocumentLimit = 50_000_000

func (i *inbound) exportCommand(ctx context.Context, msg domain.TopicMessage, key domain.Key, agent domain.Agent) error {
	if agent.Kind != domain.OpenCodeKind {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "⚠️ /export is available only for OpenCode")
	}
	if i.exportTranscript == nil || key.SessionDigest == "" || agent.Key != key {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "⚠️ export unavailable: no verified live session")
	}
	gen, err := i.topics.generation(key, msg.ThreadID)
	if err != nil {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "⚠️ export unavailable: topic changed")
	}
	data, err := i.exportTranscript(ctx, agent)
	if err != nil {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "⚠️ export failed: session unavailable, changed, or export exceeded the 64 MiB JSON limit")
	}
	if len(data) > telegramDocumentLimit {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "⚠️ export exceeds Telegram's 50 MB bot document limit; nothing was sent")
	}
	current, live := i.agents(key)
	if !live || current.Key != key || gen.ctx.Err() != nil {
		return nil
	}
	doc := domain.Document{ThreadID: msg.ThreadID, ReplyTo: msg.MessageID, Name: "opencode-transcript.md", Data: data}
	if tg, ok := i.tg.(interface {
		SendDocumentGuard(context.Context, domain.Document, domain.DispatchGuard) error
	}); ok {
		err := tg.SendDocumentGuard(ctx, doc, gen.guard)
		if err != nil && gen.ctx.Err() == nil && ctx.Err() == nil && !isFatal(err) {
			return i.reply(ctx, msg.ThreadID, msg.MessageID, "⚠️ export document could not be sent (Telegram bot limit: 50 MB); nothing was confirmed")
		}
		return i.absorb(err)
	}
	call, release, err := gen.guard(ctx)
	if err != nil {
		return nil
	}
	defer release()
	return i.absorb(i.tg.SendDocument(call, doc))
}
