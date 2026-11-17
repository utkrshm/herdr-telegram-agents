package transcript

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

// Transcript exports user and assistant text parts in order, without trimming.
// Tool, reasoning and other non-text parts are deliberately not included.
func (r *OpenCodeReader) Transcript(ctx context.Context, agent domain.Agent) ([]byte, error) {
	if agent.Kind != kindOpenCode {
		return nil, domain.ErrUnsupportedAgent
	}
	lookup := func() (domain.SessionTuple, error) {
		t, err := r.session(ctx, agent.PaneID)
		if err != nil || t.Agent != kindOpenCode || t.Kind != "id" || t.Value == "" || agent.SessionDigest == "" || t.Digest() != agent.SessionDigest {
			return domain.SessionTuple{}, errors.New("session unavailable or changed")
		}
		return t, nil
	}
	tuple, err := lookup()
	if err != nil {
		return nil, err
	}
	out, err := r.export(ctx, tuple.Value)
	if err != nil {
		return nil, errors.New("export failed or exceeded the 64 MiB JSON limit")
	}
	var doc openCodeExport
	if json.Unmarshal(out, &doc) != nil || doc.Info.ID != tuple.Value || doc.Messages == nil {
		return nil, errors.New("invalid or mismatched session export")
	}
	var markdown bytes.Buffer
	markdown.WriteString("# OpenCode transcript\n\n")
	for _, message := range doc.Messages {
		role := message.role()
		if role != "user" && role != "assistant" {
			continue
		}
		parts := message.parts()
		hasText := false
		for _, part := range parts {
			if part.Type == "text" {
				hasText = true
				break
			}
		}
		if !hasText {
			continue
		}
		if role == "user" {
			markdown.WriteString("## User\n\n")
		} else {
			markdown.WriteString("## Assistant\n\n")
		}
		for _, part := range parts {
			if part.Type == "text" {
				markdown.WriteString(part.Text)
				markdown.WriteString("\n\n")
			}
		}
	}
	if _, err := lookup(); err != nil {
		return nil, err
	}
	return markdown.Bytes(), nil
}
