package transcript

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

func TestTranscriptExactSessionAndText(t *testing.T) {
	f := &openCodeFixture{tuple: openCodeTuple, out: `{"info":{"id":"ses_abc"},"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":"  hi\n~~~go\nx\n~~~  "}]},{"info":{"role":"assistant"},"parts":[{"type":"reasoning","text":"secret"},{"type":"text","text":"reply\n"}]},{"type":"user","content":[{"type":"text","text":"last"}]}]}`}
	b, err := f.reader().Transcript(context.Background(), openCodeAgent(openCodeTuple.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	want := "# OpenCode transcript\n\n## User\n\n  hi\n~~~go\nx\n~~~  \n\n## Assistant\n\nreply\n\n\n## User\n\nlast\n\n"
	if string(b) != want {
		t.Fatalf("got %q", b)
	}
	if len(f.exported) != 1 || f.exported[0] != openCodeTuple.Value {
		t.Fatalf("exported %v", f.exported)
	}
}

func TestTranscriptFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		tuple                domain.SessionTuple
		digest, output, kind string
		lookupErr            error
	}{
		{"missing", domain.SessionTuple{}, openCodeTuple.Digest(), openCodeTurn, "opencode", nil},
		{"lookup error", openCodeTuple, openCodeTuple.Digest(), openCodeTurn, "opencode", errors.New("private")},
		{"stale", openCodeTuple, "other", openCodeTurn, "opencode", nil},
		{"no digest", openCodeTuple, "", openCodeTurn, "opencode", nil},
		{"wrong export", openCodeTuple, openCodeTuple.Digest(), strings.Replace(openCodeTurn, `"id": "ses_abc"`, `"id": "other"`, 1), "opencode", nil},
		{"missing messages", openCodeTuple, openCodeTuple.Digest(), `{"info":{"id":"ses_abc"}}`, "opencode", nil},
		{"unsupported", openCodeTuple, openCodeTuple.Digest(), openCodeTurn, "claude", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &openCodeFixture{tuple: tc.tuple, lookErr: tc.lookupErr, out: tc.output}
			a := openCodeAgent(tc.digest)
			a.Kind = tc.kind
			if _, err := f.reader().Transcript(context.Background(), a); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
	f := &openCodeFixture{tuple: openCodeTuple, out: openCodeTurn}
	f.exportFn = func(string) ([]byte, error) { f.tuple.Value = "replacement"; return []byte(openCodeTurn), nil }
	if _, err := f.reader().Transcript(context.Background(), openCodeAgent(openCodeTuple.Digest())); err == nil {
		t.Fatal("session changed during export")
	}
}
