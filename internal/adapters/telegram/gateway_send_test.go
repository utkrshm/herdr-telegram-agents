package telegram_test

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/permgps/herdr-telegram-agents/internal/adapters/telegram"
	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

func TestSendSinglePart(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })

	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: `a < b & "c"`}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 1 {
		t.Fatalf("calls = %d", len(calls))
	}
	f := calls[0].form
	if f.Get("text") != "a &lt; b &amp; &#34;c&#34;" {
		t.Errorf("text = %q", f.Get("text"))
	}
	if f.Get("chat_id") != "-1001234567890" || f.Get("message_thread_id") != "42" ||
		f.Get("parse_mode") != "HTML" || f.Get("disable_notification") != "true" {
		t.Errorf("form = %v", f)
	}
	if f.Has("reply_parameters") {
		t.Errorf("reply_parameters sent without ReplyTo: %v", f)
	}
}

func TestSendLiteralSuffixStaysInSource(t *testing.T) {
	for _, text := range []string{"example\n… (0/2)", strings.Repeat("x", 4500) + "\n… (0/2)"} {
		h := newHarness(t)
		h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
		if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, Code: true}); err != nil {
			t.Fatal(err)
		}
		var content strings.Builder
		for _, call := range h.api.callsOf("sendMessage") {
			c, _ := codeMessageParts(call.form.Get("text"))
			content.WriteString(c)
		}
		if content.String() != text {
			t.Fatalf("source changed: got tail %q", content.String()[len(content.String())-20:])
		}
	}
}

func TestSendFormattedUnicodeHasBoundedParts(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
	text := strings.Repeat("🌍 world. ", 1500) + "\n… (0/2)"
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, Markdown: true}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) < 2 {
		t.Fatal("expected multipart")
	}
	var joined strings.Builder
	for _, c := range calls {
		body := c.form.Get("text")
		if len(utf16.Encode([]rune(body))) > 4000 {
			t.Fatalf("oversized formatted part: %d", len(utf16.Encode([]rune(body))))
		}
		idx := strings.LastIndex(body, "\n… (")
		if idx < 0 {
			t.Fatal("missing generated marker")
		}
		joined.WriteString(body[:idx])
	}
	if joined.String() != text {
		t.Fatalf("formatted content changed: got %d bytes want %d", joined.Len(), len(text))
	}
}

func TestSendGuardRechecksEveryPartAndReturnsAcknowledgedID(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 71}) })
	stale := errors.New("mapping changed")
	checks := 0
	guard := func(ctx context.Context) (context.Context, context.CancelFunc, error) {
		checks++
		if checks > 1 {
			return nil, nil, stale
		}
		return ctx, func() {}, nil
	}
	id, err := h.gw.SendGuard(h.ctx, domain.Outgoing{ThreadID: 42, Text: strings.Repeat("x", 8000)}, guard)
	if id != 71 || !errors.Is(err, stale) {
		t.Fatalf("SendGuard = %d, %v", id, err)
	}
	if calls := len(h.api.callsOf("sendMessage")); calls != 1 {
		t.Fatalf("sent %d parts after reassignment", calls)
	}
}

func TestSendToGeneralOmitsThread(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })

	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 0, Text: "started"}); err != nil {
		t.Fatal(err)
	}
	f := h.api.callsOf("sendMessage")[0].form
	if f.Has("message_thread_id") {
		t.Errorf("message_thread_id must be omitted for General: %v", f)
	}
}

func TestSendNotifyAndReply(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })

	text := strings.Repeat("a", 4032) + "\n" + strings.Repeat("b", 10)
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, ReplyTo: 77, Notify: true}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	first, second := calls[0].form, calls[1].form
	if first.Has("disable_notification") || second.Has("disable_notification") {
		t.Errorf("Notify must not disable notification: %v / %v", first, second)
	}
	rp := first.Get("reply_parameters")
	if !strings.Contains(rp, `"message_id":77`) || !strings.Contains(rp, `"allow_sending_without_reply":true`) {
		t.Errorf("first part reply_parameters = %q", rp)
	}
	if second.Has("reply_parameters") {
		t.Errorf("second part must not quote the message: %v", second)
	}
}

func TestSendHTMLPassesThrough(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 0, Text: `<a href="https://t.me/c/1/2">a &amp; b</a>`, HTML: true}); err != nil {
		t.Fatal(err)
	}
	if got := h.api.callsOf("sendMessage")[0].form.Get("text"); got != `<a href="https://t.me/c/1/2">a &amp; b</a>` {
		t.Errorf("text = %q", got)
	}
}

func TestSendCodeAndChunks(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })

	lines := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		lines = append(lines, strings.Repeat("x", 30)+" <"+itoa(i)+">")
	}
	text := strings.Join(lines, "\n") // ~10.8k runes -> 3 parts
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, Code: true}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 3 {
		t.Fatalf("calls = %d, want 3", len(calls))
	}
	var joined []string
	for i, c := range calls {
		body := c.form.Get("text")
		content, suffix := codeMessageParts(body)
		if content == "" || !strings.Contains(body, "</pre>") {
			t.Errorf("part %d not wrapped in <pre>: %.20q", i+1, body)
		}
		if strings.Contains(content, "<0>") {
			t.Errorf("part %d contains unescaped angle brackets", i+1)
		}
		joined = append(joined, content)
		if !strings.Contains(suffix, "… ("+itoa(i+1)+"/3)") {
			t.Errorf("part %d suffix = %q", i+1, suffix)
		}
	}
	if got := strings.Join(joined, ""); got != strings.ReplaceAll(strings.ReplaceAll(text, "<", "&lt;"), ">", "&gt;") {
		t.Error("parts do not reassemble into the escaped text")
	}
}

func TestSendLongCodeReplyWithoutPartCap(t *testing.T) {
	h := newHarness(t)
	nextID := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		nextID++
		return okReply(map[string]any{"message_id": nextID})
	})

	lines := make([]string, 8)
	for i := range lines {
		lines[i] = strings.Repeat(string(rune('a'+i)), 4032)
	}
	text := strings.Join(lines, "\n")
	id, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, Code: true})
	if err != nil {
		t.Fatalf("Send = %d, %v", id, err)
	}

	calls := h.api.callsOf("sendMessage")
	if len(calls) <= 5 {
		t.Fatalf("send count = %d, want more than five parts", len(calls))
	}
	parts := make([]string, 0, len(calls))
	for i, call := range calls {
		body := call.form.Get("text")
		content, suffix := codeMessageParts(body)
		if !strings.Contains(body, "</pre>") {
			t.Fatalf("part %d is not a complete code block", i+1)
		}
		if !strings.Contains(suffix, "… (") {
			t.Fatalf("part %d has no response marker", i+1)
		}
		parts = append(parts, content)
	}
	want := strings.ReplaceAll(strings.ReplaceAll(text, "<", "&lt;"), ">", "&gt;")
	if got := strings.Join(parts, ""); got != want {
		t.Fatalf("reassembled %d characters, want %d; got tail %q, want tail %q", len(got), len(want), got[len(got)-20:], want[len(want)-20:])
	}
}

func TestSendUnicodeResponsePartsStayWithinFourThousandUTF16Units(t *testing.T) {
	h := newHarness(t)
	nextID := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		nextID++
		return okReply(map[string]any{"message_id": nextID})
	})
	text := strings.Repeat("🌍", 50000)
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, Code: true}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) <= 20 {
		t.Fatalf("parts = %d, want more than twenty", len(calls))
	}
	var rebuilt strings.Builder
	for i, call := range calls {
		body := call.form.Get("text")
		content, suffix := codeMessageParts(body)
		if units := len(utf16.Encode([]rune(content + suffix))); units > 4000 {
			t.Errorf("part %d uses %d UTF-16 units including suffix; want <= 4000", i+1, units)
		}
		rebuilt.WriteString(content)
		wantSuffix := "… (" + itoa(i+1) + "/" + itoa(len(calls)) + ")"
		if !strings.HasSuffix(suffix, wantSuffix) {
			t.Errorf("part %d suffix = %q, want %q", i+1, suffix, wantSuffix)
		}
	}
	if rebuilt.String() != text {
		t.Fatalf("unicode response changed during split/reassembly: got %d runes, want %d", len([]rune(rebuilt.String())), len([]rune(text)))
	}
}

func TestSendPrefersSentenceBoundaryWhenSplitting(t *testing.T) {
	h := newHarness(t)
	nextID := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		nextID++
		return okReply(map[string]any{"message_id": nextID})
	})
	first := strings.Repeat("first sentence ", 220) + "done. "
	second := strings.Repeat("second sentence ", 220) + "done."
	text := first + second
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 2 {
		t.Fatalf("parts = %d, want two sentence-sized parts", len(calls))
	}
	firstPart := stripResponseSuffix(calls[0].form.Get("text"))
	if !strings.HasSuffix(firstPart, ".") {
		t.Fatalf("first part ended mid-sentence: %q", firstPart[len(firstPart)-40:])
	}
	joined := stripResponseSuffix(calls[0].form.Get("text")) + stripResponseSuffix(calls[1].form.Get("text"))
	if joined != text {
		t.Fatal("sentence split changed or dropped source text")
	}
}

func TestSendLongReplyAfterQueueWindowIsPreused(t *testing.T) {
	h := newHarness(t)
	nextID := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		nextID++
		return okReply(map[string]any{"message_id": nextID})
	})

	// Spend the whole per-minute allowance before beginning the long reply.
	for i := 0; i < 20; i++ {
		if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "prior"}); err != nil {
			t.Fatalf("preuse send %d: %v", i+1, err)
		}
	}
	lines := make([]string, 24)
	for i := range lines {
		lines[i] = strings.Repeat(string(rune('a'+i%26)), 4032)
	}
	text := strings.Join(lines, "\n")
	guardCalls := 0
	guard := func(ctx context.Context) (context.Context, context.CancelFunc, error) {
		guardCalls++
		return ctx, func() {}, nil
	}
	id, err := h.gw.SendAt(h.ctx, domain.TopicAddress{ChatID: testChatID, ThreadID: 42}, domain.Outgoing{Text: text, Code: true}, guard)
	if err != nil {
		t.Fatalf("long SendAt = %d, %v", id, err)
	}
	calls := h.api.callsOf("sendMessage")
	partCount := len(calls) - 20
	if partCount <= 20 {
		t.Fatalf("long response split into %d parts, want more than 20", partCount)
	}
	if id != len(calls) {
		t.Fatalf("final id = %d, want %d", id, len(calls))
	}
	if guardCalls != partCount {
		t.Fatalf("dispatch guard called %d times, want once per output part (%d)", guardCalls, partCount)
	}
	parts := make([]string, 0, len(lines))
	for _, call := range calls[20:] {
		body := call.form.Get("text")
		content, _ := codeMessageParts(body)
		parts = append(parts, content)
	}
	if got := strings.Join(parts, ""); got != text {
		t.Fatalf("reassembled long reply length %d, want %d; tail was lost or duplicated", len(got), len(text))
	}
	waits := h.sleep.recorded()
	foundWindowWait := false
	for _, wait := range waits {
		if wait >= 40*time.Second {
			foundWindowWait = true
			break
		}
	}
	if !foundWindowWait {
		t.Fatalf("queue did not enforce the preused per-minute window; waits = %v", waits)
	}
}

func codeMessageParts(body string) (string, string) {
	content := strings.TrimPrefix(body, "<pre>")
	close := strings.Index(content, "</pre>")
	if close < 0 {
		return content, ""
	}
	return content[:close], content[close+len("</pre>"):]
}

func stripResponseSuffix(text string) string {
	index := strings.LastIndex(text, "\n… (")
	if index < 0 || !strings.HasSuffix(text, ")") {
		return text
	}
	return text[:index]
}

func TestSendStopsAtFirstFailure(t *testing.T) {
	h := newHarness(t)
	n := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		n++
		if n == 2 {
			return errReply(400, "Bad Request: message thread not found")
		}
		return okReply(map[string]any{"message_id": n})
	})
	text := strings.Repeat("a", 4032) + "\n" + strings.Repeat("b", 4032) + "\n" + strings.Repeat("c", 10)
	_, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text})
	if !errors.Is(err, domain.ErrTopicGone) {
		t.Fatalf("err = %v, want ErrTopicGone", err)
	}
	if !strings.Contains(err.Error(), "thread 42 part 2/3") {
		t.Errorf("err = %v, want thread and part in message", err)
	}
	if got := len(h.api.callsOf("sendMessage")); got != 2 {
		t.Errorf("calls = %d, want 2 (third part must not be sent)", got)
	}
}

func TestSendClosedTopic(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return errReply(400, "Bad Request: TOPIC_CLOSED") })
	_, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "hi"})
	if !errors.Is(err, domain.ErrTopicClosed) {
		t.Fatalf("err = %v, want ErrTopicClosed", err)
	}
}

func TestSendEmptyIsNoop(t *testing.T) {
	h := newHarness(t)
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42}); err != nil {
		t.Fatal(err)
	}
	if n := len(h.api.methods()); n != 0 {
		t.Errorf("%d calls for empty text", n)
	}
}

func TestReact(t *testing.T) {
	h := newHarness(t)
	if err := h.gw.React(h.ctx, 42, 77, "👍"); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("setMessageReaction")
	if len(calls) != 1 {
		t.Fatalf("calls = %d", len(calls))
	}
	f := calls[0].form
	if f.Get("chat_id") != "-1001234567890" || f.Get("message_id") != "77" {
		t.Errorf("form = %v", f)
	}
	if r := f.Get("reaction"); !strings.Contains(r, `"type":"emoji"`) || !strings.Contains(r, `"emoji":"👍"`) {
		t.Errorf("reaction = %q", r)
	}
	if f.Has("message_thread_id") {
		t.Errorf("reactions carry no thread id: %v", f)
	}
}

func TestReactFailureIsTranslated(t *testing.T) {
	h := newHarness(t)
	h.api.on("setMessageReaction", func(url.Values) apiReply { return errReply(400, "Bad Request: REACTION_INVALID") })
	err := h.gw.React(h.ctx, 42, 77, "🙃")
	if err == nil || errors.Is(err, domain.ErrTopicGone) || errors.Is(err, domain.ErrTopicClosed) {
		t.Fatalf("err = %v, want a plain API error", err)
	}
	if !strings.Contains(err.Error(), "setMessageReaction") {
		t.Errorf("err = %v, want the method name", err)
	}
}

func TestSendDocument(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendDocument", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 5}) })
	body := []byte("line one\nline two\n")
	doc := domain.Document{ThreadID: 42, Name: "screen-w1-p1-101500.txt", Data: body, Caption: "2 lines since your last message", ReplyTo: 7}
	if err := h.gw.SendDocument(h.ctx, doc); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendDocument")
	if len(calls) != 1 {
		t.Fatalf("sendDocument calls = %d, want 1", len(calls))
	}
	c := calls[0]
	if got := c.form.Get("message_thread_id"); got != "42" {
		t.Errorf("message_thread_id = %q", got)
	}
	if got := c.form.Get("caption"); got != doc.Caption {
		t.Errorf("caption = %q", got)
	}
	if got := c.form.Get("disable_notification"); got != "true" {
		t.Errorf("disable_notification = %q, want true", got)
	}
	if got := c.form.Get("disable_content_type_detection"); got != "true" {
		t.Errorf("disable_content_type_detection = %q, want true", got)
	}
	if !strings.Contains(c.form.Get("reply_parameters"), `"message_id":7`) {
		t.Errorf("reply_parameters = %q", c.form.Get("reply_parameters"))
	}
	file, ok := c.files["document"]
	if !ok {
		t.Fatalf("no document file part; files = %v", c.files)
	}
	if file.name != doc.Name || string(file.data) != string(body) {
		t.Errorf("file = %q %q", file.name, file.data)
	}
	if strings.Contains(h.buf.String(), "line one") {
		t.Error("document body must not be logged")
	}
}

func TestSendDocumentToGeneralAndErrors(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendDocument", func(form url.Values) apiReply {
		if form.Get("caption") == "fail" {
			return errReply(400, "Bad Request: message thread not found")
		}
		return okReply(map[string]any{"message_id": 6})
	})
	if err := h.gw.SendDocument(h.ctx, domain.Document{Name: "general.txt", Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if got := h.api.callsOf("sendDocument")[0].form.Get("message_thread_id"); got != "" {
		t.Errorf("General must omit message_thread_id, got %q", got)
	}
	err := h.gw.SendDocument(h.ctx, domain.Document{ThreadID: 9, Name: "x.txt", Data: []byte("x"), Caption: "fail"})
	if !errors.Is(err, domain.ErrTopicGone) {
		t.Fatalf("err = %v, want ErrTopicGone", err)
	}
}

func TestSendPhotoInlineToTopic(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendPhoto", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 7}) })
	photo := domain.Photo{ThreadID: 42, Name: "plot.png", Data: []byte("png bytes"), Caption: "chart"}
	if err := h.gw.SendPhoto(h.ctx, photo); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendPhoto")
	if len(calls) != 1 {
		t.Fatalf("sendPhoto calls=%d", len(calls))
	}
	call := calls[0]
	if call.form.Get("message_thread_id") != "42" || call.form.Get("caption") != "chart" || call.form.Get("disable_notification") != "true" {
		t.Fatalf("form=%v", call.form)
	}
	file, ok := call.files["photo"]
	if !ok || file.name != photo.Name || string(file.data) != string(photo.Data) {
		t.Fatalf("photo part=%+v", file)
	}
}

func TestSendPhotoGuardRunsAfterQueuePacingAndRejectsChangedMapping(t *testing.T) {
	api := newFakeAPI(t)
	log := slog.New(slog.DiscardHandler)
	api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
	entered, release := make(chan struct{}), make(chan struct{})
	queue := telegram.NewQueue(log, telegram.QueueConfig{Sleep: func(ctx context.Context, _ time.Duration) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	gw := telegram.NewGateway(api.bot(t, log, nil), telegram.Config{ChatID: testChatID}, queue, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); gw.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	if _, err := gw.Send(ctx, domain.Outgoing{ThreadID: 1, Text: "prior call"}); err != nil {
		t.Fatal(err)
	}
	var mappingChanged atomic.Bool
	result := make(chan error, 1)
	go func() {
		result <- gw.SendPhotoGuard(ctx, domain.Photo{ThreadID: 1, Name: "image.png", Data: []byte("image")}, func(ctx context.Context) (context.Context, context.CancelFunc, error) {
			if mappingChanged.Load() {
				return nil, nil, errors.New("topic mapping changed")
			}
			return ctx, func() {}, nil
		})
	}()
	<-entered
	mappingChanged.Store(true)
	close(release)
	if err := <-result; err == nil || !strings.Contains(err.Error(), "topic mapping changed") {
		t.Fatalf("send error=%v", err)
	}
	if calls := api.callsOf("sendPhoto"); len(calls) != 0 {
		t.Fatalf("sendPhoto calls=%d", len(calls))
	}
}

func TestPhotoMalformedAcknowledgementIsNotRetried(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendPhoto", func(url.Values) apiReply { return okReply(map[string]any{"message_id": "malformed"}) })
	err := h.gw.SendPhoto(h.ctx, domain.Photo{ThreadID: 42, Name: "image.png", Data: []byte("image")})
	if err == nil {
		t.Fatal("malformed acknowledgement unexpectedly succeeded")
	}
	if calls := h.api.callsOf("sendPhoto"); len(calls) != 1 {
		t.Fatalf("sendPhoto attempts=%d, want exactly one", len(calls))
	}
}

func TestDocumentMalformedAcknowledgementIsNotRetried(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendDocument", func(url.Values) apiReply { return okReply(map[string]any{"message_id": "malformed"}) })
	err := h.gw.SendOriginalDocument(h.ctx, domain.Document{ThreadID: 42, Name: "file.bin", Data: []byte{0, 0xff}}, nil)
	if err == nil {
		t.Fatal("malformed acknowledgement unexpectedly succeeded")
	}
	if calls := h.api.callsOf("sendDocument"); len(calls) != 1 {
		t.Fatalf("sendDocument attempts=%d, want exactly one", len(calls))
	}
}

func TestSendAttachesKeyboardToLastPart(t *testing.T) {
	h := newHarness(t)
	n := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		n++
		return okReply(map[string]any{"message_id": 500 + n})
	})

	text := strings.Repeat("a", 4032) + "\n" + strings.Repeat("b", 4032) + "\n" + strings.Repeat("c", 10)
	buttons := []domain.Button{{Text: "1️⃣ Red", Data: "1"}, {Text: "2️⃣ Green", Data: "2"}}
	id, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, Code: true, Buttons: buttons})
	if err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 3 {
		t.Fatalf("calls = %d, want 3", len(calls))
	}
	if id != 503 {
		t.Errorf("returned id = %d, want the last part's 503", id)
	}
	for i, c := range calls[:2] {
		if c.form.Has("reply_markup") {
			t.Errorf("part %d carries reply_markup: %v", i+1, c.form.Get("reply_markup"))
		}
	}
	markup := calls[2].form.Get("reply_markup")
	for _, want := range []string{`"inline_keyboard":[[{"text":"1️⃣ Red","callback_data":"1"}],[{"text":"2️⃣ Green","callback_data":"2"}]]`} {
		if !strings.Contains(markup, want) {
			t.Errorf("reply_markup = %s, want %s", markup, want)
		}
	}
}

func TestSendReturnsMessageID(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 321}) })

	id, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if id != 321 {
		t.Errorf("id = %d, want 321", id)
	}
	if h.api.callsOf("sendMessage")[0].form.Has("reply_markup") {
		t.Errorf("reply_markup sent without Buttons")
	}
}

func TestEditButtonsReplacesAndRemoves(t *testing.T) {
	h := newHarness(t)
	h.api.on("editMessageReplyMarkup", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 7}) })

	if err := h.gw.EditButtons(h.ctx, 7, []domain.Button{{Text: "✅ 2 · Green", Data: "done"}}); err != nil {
		t.Fatal(err)
	}
	if err := h.gw.EditButtons(h.ctx, 7, nil); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("editMessageReplyMarkup")
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	f := calls[0].form
	if f.Get("chat_id") != "-1001234567890" || f.Get("message_id") != "7" || !strings.Contains(f.Get("reply_markup"), `"callback_data":"done"`) {
		t.Errorf("first edit form = %v", f)
	}
	if got := calls[1].form.Get("reply_markup"); !strings.Contains(got, `"inline_keyboard":[]`) {
		t.Errorf("removal reply_markup = %s, want an empty inline_keyboard", got)
	}
}

func TestEditButtonsNotModifiedIsNil(t *testing.T) {
	h := newHarness(t)
	h.api.on("editMessageReplyMarkup", func(url.Values) apiReply {
		return errReply(400, "Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message")
	})
	if err := h.gw.EditButtons(h.ctx, 7, nil); err != nil {
		t.Fatalf("not-modified must be success, got %v", err)
	}
}

func TestEditButtonsOtherErrorIsReturned(t *testing.T) {
	h := newHarness(t)
	h.api.on("editMessageReplyMarkup", func(url.Values) apiReply { return errReply(400, "Bad Request: message can't be edited") })
	err := h.gw.EditButtons(h.ctx, 7, nil)
	var api *telegram.APIError
	if !errors.As(err, &api) || api.Code != 400 {
		t.Fatalf("err = %v, want *APIError 400", err)
	}
}

func TestAnswerButton(t *testing.T) {
	h := newHarness(t)
	if err := h.gw.AnswerButton(h.ctx, "cb-1", "sent: 2"); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("answerCallbackQuery")
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	f := calls[0].form
	if f.Get("callback_query_id") != "cb-1" || f.Get("text") != "sent: 2" {
		t.Errorf("form = %v", f)
	}
}

func TestEditTextSendsMarkupAndHTML(t *testing.T) {
	h := newHarness(t)
	h.api.on("editMessageText", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 7}) })

	buttons := []domain.Button{{Text: "☑ Sync", Data: "o:t:sync.enabled"}, {Text: "‹ Back", Data: "o:h"}}
	if err := h.gw.EditText(h.ctx, 7, "<b>Sync</b>", true, buttons); err != nil {
		t.Fatal(err)
	}
	if err := h.gw.EditText(h.ctx, 7, "plain", false, nil); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("editMessageText")
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	f := calls[0].form
	if f.Get("chat_id") != "-1001234567890" || f.Get("message_id") != "7" || f.Get("text") != "<b>Sync</b>" || f.Get("parse_mode") != "HTML" {
		t.Errorf("first edit form = %v", f)
	}
	if !strings.Contains(f.Get("reply_markup"), `"callback_data":"o:t:sync.enabled"`) {
		t.Errorf("reply_markup = %s", f.Get("reply_markup"))
	}
	if g := calls[1].form; g.Get("parse_mode") != "" || !strings.Contains(g.Get("reply_markup"), `"inline_keyboard":[]`) {
		t.Errorf("second edit form = %v", g)
	}
}

func TestEditTextNotModifiedIsNil(t *testing.T) {
	h := newHarness(t)
	h.api.on("editMessageText", func(url.Values) apiReply {
		return errReply(400, "Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message")
	})
	if err := h.gw.EditText(h.ctx, 7, "same", false, nil); err != nil {
		t.Fatalf("not-modified must be success, got %v", err)
	}
}

func TestEditTextOtherErrorIsReturned(t *testing.T) {
	h := newHarness(t)
	h.api.on("editMessageText", func(url.Values) apiReply { return errReply(400, "Bad Request: message can't be edited") })
	err := h.gw.EditText(h.ctx, 7, "x", false, nil)
	var api *telegram.APIError
	if !errors.As(err, &api) || api.Code != 400 {
		t.Fatalf("err = %v, want *APIError 400", err)
	}
}

func TestInlineKeyboardGroupsRows(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })

	buttons := []domain.Button{
		{Text: "a", Data: "1", Row: 1}, {Text: "b", Data: "2", Row: 1}, {Text: "c", Data: "3", Row: 1},
		{Text: "d", Data: "4", Row: 2},
		{Text: "e", Data: "5"}, {Text: "f", Data: "6"},
	}
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 0, Text: "grid", Buttons: buttons}); err != nil {
		t.Fatal(err)
	}
	markup := h.api.callsOf("sendMessage")[0].form.Get("reply_markup")
	want := `[[{"text":"a","callback_data":"1"},{"text":"b","callback_data":"2"},{"text":"c","callback_data":"3"}],[{"text":"d","callback_data":"4"}],[{"text":"e","callback_data":"5"}],[{"text":"f","callback_data":"6"}]]`
	if !strings.Contains(markup, want) {
		t.Errorf("reply_markup = %s\nwant %s", markup, want)
	}
}

func TestSendMarkdownRenders(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 5}) })
	id, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "**hi** `a<b`\n- one", Markdown: true})
	if err != nil || id != 5 {
		t.Fatalf("Send = %d, %v", id, err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 1 {
		t.Fatalf("calls = %d", len(calls))
	}
	if got := calls[0].form.Get("text"); got != "<b>hi</b> <code>a&lt;b</code>\n• one" {
		t.Errorf("text = %q", got)
	}
	if got := calls[0].form.Get("parse_mode"); got != "HTML" {
		t.Errorf("parse_mode = %q", got)
	}
}

func TestSendMarkdownFallsBackToPre(t *testing.T) {
	h := newHarness(t)
	n := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		n++
		if n == 1 {
			return errReply(400, `Bad Request: can't parse entities: Unsupported start tag "x" at byte offset 1`)
		}
		return okReply(map[string]any{"message_id": 7})
	})
	id, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "**hi** <x>", Markdown: true})
	if err != nil || id != 7 {
		t.Fatalf("Send = %d, %v", id, err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want the rejected part and its <pre> retry", len(calls))
	}
	if got := calls[1].form.Get("text"); got != "<pre>**hi** &lt;x&gt;</pre>" {
		t.Errorf("retry text = %q", got)
	}
	if !strings.Contains(h.buf.String(), "markdown rejected, re-sent as pre") {
		t.Errorf("no warn line in log: %s", h.buf.String())
	}
}

func TestSendMarkdownTooLongFallsBackToPre(t *testing.T) {
	h := newHarness(t)
	n := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		n++
		if n == 1 {
			return errReply(400, "Bad Request: message is too long")
		}
		return okReply(map[string]any{"message_id": 9})
	})
	id, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "**x**", Markdown: true})
	if err != nil || id != 9 || len(h.api.callsOf("sendMessage")) != 2 {
		t.Fatalf("Send = %d, %v, calls = %d", id, err, len(h.api.callsOf("sendMessage")))
	}
}

func TestSendMarkdownFallbackFailsOnce(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply {
		return errReply(400, "Bad Request: can't parse entities: boom")
	})
	_, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "x", Markdown: true})
	if err == nil {
		t.Fatal("second rejection must surface")
	}
	if got := len(h.api.callsOf("sendMessage")); got != 2 {
		t.Errorf("calls = %d, want exactly one retry", got)
	}
}

func TestSendMaxPartsTrailer(t *testing.T) {
	h := newHarness(t)
	n := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		n++
		return okReply(map[string]any{"message_id": n})
	})
	text := strings.Repeat("a", 4032) + "\n" + strings.Repeat("b", 4032) + "\n" + strings.Repeat("c", 10)
	id, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, Code: true, MaxParts: 2})
	if err != nil || id != 2 {
		t.Fatalf("Send = %d, %v", id, err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	if got := calls[1].form.Get("text"); !strings.HasSuffix(got, "… (+107 chars)") || !strings.Contains(got, "</pre>\n… (2/2)") {
		t.Errorf("last part = %.40q (tail %q)", got, got[len(got)-30:])
	}
	if got := calls[0].form.Get("text"); strings.Contains(got, "chars)") {
		t.Error("trailer on a non-final part")
	}
}

func TestSendMarkdownMaxPartsTrailer(t *testing.T) {
	h := newHarness(t)
	n := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		n++
		return okReply(map[string]any{"message_id": n})
	})
	text := "```go\n" + strings.Repeat("🌍 line\n", 20000) + "```"
	_, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, Markdown: true, MaxParts: 5})
	if err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) == 0 || len(calls) > 5 {
		t.Fatalf("calls = %d, want 1..5", len(calls))
	}
	last := calls[len(calls)-1].form.Get("text")
	if !strings.Contains(last, "chars)") || !strings.Contains(last, "</code></pre>") {
		t.Fatalf("missing truncation trailer or closing fence: %.100q", last)
	}
}

func TestSendMarkdownFenceAcrossParts(t *testing.T) {
	h := newHarness(t)
	n := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		n++
		return okReply(map[string]any{"message_id": n})
	})
	var b strings.Builder
	b.WriteString("Result:\n```go\n")
	for i := 0; i < 300; i++ {
		b.WriteString(strings.Repeat("x", 39) + "\n")
	}
	b.WriteString("```\ndone")
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: b.String(), Markdown: true}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) < 3 {
		t.Fatalf("calls = %d, want several parts", len(calls))
	}
	for i, c := range calls {
		body := c.form.Get("text")
		if !strings.Contains(body, `<pre><code class="language-go">`) || !strings.Contains(body, "</code></pre>") {
			t.Errorf("part %d lacks a complete code block: %.60q", i+1, body)
		}
		if strings.Contains(body, "```") {
			t.Errorf("part %d leaks a raw fence", i+1)
		}
		wantSuffix := "… (" + itoa(i+1) + "/" + itoa(len(calls)) + ")"
		if !strings.Contains(body, wantSuffix) {
			t.Errorf("part %d lacks remaining/total suffix %q: tail %q", i+1, wantSuffix, body[max(0, len(body)-100):])
		}
	}
	if !strings.HasSuffix(calls[len(calls)-1].form.Get("text"), "</code></pre>\ndone\n… (4/4)") {
		t.Errorf("last part = %q", calls[len(calls)-1].form.Get("text"))
	}
}

func TestSendForceReply(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 7}) })
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "✏️ send the text", ReplyTo: 5, ForceReply: true}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 1 {
		t.Fatalf("calls = %d", len(calls))
	}
	markup := calls[0].form.Get("reply_markup")
	if !strings.Contains(markup, `"force_reply":true`) || !strings.Contains(markup, `"selective":true`) {
		t.Fatalf("reply_markup = %q", markup)
	}
	// Buttons win over a force reply.
	h.api.calls = nil
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "q", ForceReply: true, Buttons: []domain.Button{{Text: "1", Data: "1"}}}); err != nil {
		t.Fatal(err)
	}
	markup = h.api.callsOf("sendMessage")[0].form.Get("reply_markup")
	if strings.Contains(markup, "force_reply") || !strings.Contains(markup, "inline_keyboard") {
		t.Fatalf("reply_markup with buttons = %q", markup)
	}
}

func TestSendFooter(t *testing.T) {
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
	// A single Code part: the footer follows the </pre> on its own line.
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "$ make\nok", Code: true, Footer: "⏱ 4 min · fable-5-1"}); err != nil {
		t.Fatal(err)
	}
	if got := h.api.callsOf("sendMessage")[0].form.Get("text"); got != "<pre>$ make\nok</pre>\n⏱ 4 min · fable-5-1" {
		t.Errorf("text = %q", got)
	}
	if !strings.Contains(h.buf.String(), "footer=true") || !strings.Contains(h.buf.String(), "limit=") {
		t.Errorf("log lacks footer/limit fields: %s", h.buf.String())
	}
	// Three parts: the footer rides on the last one only.
	lines := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		lines = append(lines, strings.Repeat("x", 30)+" "+itoa(i))
	}
	h = newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: strings.Join(lines, "\n"), Code: true, Footer: "⏱ 4 min"}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 3 {
		t.Fatalf("calls = %d, want 3", len(calls))
	}
	for i, c := range calls[:2] {
		if body := c.form.Get("text"); !strings.Contains(body, "</pre>") || strings.Contains(body, "⏱") {
			t.Errorf("part %d carries a footer: %.40q", i+1, body[len(body)-40:])
		}
	}
	if body := calls[2].form.Get("text"); !strings.HasSuffix(body, "\n⏱ 4 min") || !strings.Contains(body, "</pre>") {
		t.Errorf("last part lacks the footer: %.40q", body[len(body)-40:])
	}
	// The footer is escaped, and a long one is cut to 200 units.
	h = newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "x", Footer: "<b>bold</b> & co"}); err != nil {
		t.Fatal(err)
	}
	if got := h.api.callsOf("sendMessage")[0].form.Get("text"); got != "x\n&lt;b&gt;bold&lt;/b&gt; &amp; co" {
		t.Errorf("escaped footer = %q", got)
	}
	long := strings.Repeat("😀", 150) // 300 UTF-16 units
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "y", Footer: long}); err != nil {
		t.Fatal(err)
	}
	got := h.api.callsOf("sendMessage")[1].form.Get("text")
	footer := strings.TrimPrefix(got, "y\n")
	if !strings.HasSuffix(footer, "…") || len([]rune(footer)) != 100 {
		t.Errorf("long footer = %d runes, %q…", len([]rune(footer)), footer[:20])
	}
	// A text of exactly textMax units with a 30-unit footer splits in two
	// and no part exceeds Telegram's 4096 units.
	h = newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
	text := strings.Repeat("a", 4096-64)
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: text, Footer: strings.Repeat("f", 30)}); err != nil {
		t.Fatal(err)
	}
	calls = h.api.callsOf("sendMessage")
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	for i, c := range calls {
		if n := len([]rune(c.form.Get("text"))); n > 4096 {
			t.Errorf("part %d is %d units", i+1, n)
		}
	}
	if !strings.HasSuffix(calls[1].form.Get("text"), "\n"+strings.Repeat("f", 30)) {
		t.Error("footer missing on the second part")
	}
}

func TestSendFold(t *testing.T) {
	lines := func(n int) string {
		out := make([]string, n)
		for i := range out {
			out[i] = "line " + itoa(i)
		}
		return strings.Join(out, "\n")
	}
	for _, tc := range []struct {
		name   string
		lines  int
		fold   int
		folded bool
	}{
		{"over the threshold", 25, 20, true},
		{"at the threshold", 20, 20, false},
		{"fold off", 25, 0, false},
	} {
		h := newHarness(t)
		h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
		if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: lines(tc.lines), Code: true, Fold: tc.fold, Footer: "⏱ 1 min"}); err != nil {
			t.Fatal(err)
		}
		got := h.api.callsOf("sendMessage")[0].form.Get("text")
		want := "<pre>" + lines(tc.lines) + "</pre>\n⏱ 1 min"
		if tc.folded {
			want = "<blockquote expandable><pre>" + lines(tc.lines) + "</pre></blockquote>\n⏱ 1 min"
		}
		if got != want {
			t.Errorf("%s: text = %.60q", tc.name, got)
		}
		if strings.Contains(h.buf.String(), "folded=true") != tc.folded {
			t.Errorf("%s: log folded flag: %s", tc.name, h.buf.String())
		}
	}
	// Markdown over the threshold: the rendered code block sits inside the
	// quote.
	h := newHarness(t)
	h.api.on("sendMessage", func(url.Values) apiReply { return okReply(map[string]any{"message_id": 1}) })
	md := "**done**\n```go\n" + lines(25) + "\n```"
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: md, Markdown: true, Fold: 20}); err != nil {
		t.Fatal(err)
	}
	got := h.api.callsOf("sendMessage")[0].form.Get("text")
	if !strings.HasPrefix(got, "<blockquote expandable><b>done</b>\n<pre><code class=\"language-go\">") || !strings.HasSuffix(got, "</code></pre></blockquote>") {
		t.Errorf("markdown fold = %q", got)
	}
	// The Markdown fallback keeps the fold and the footer.
	h = newHarness(t)
	n := 0
	h.api.on("sendMessage", func(url.Values) apiReply {
		n++
		if n == 1 {
			return errReply(400, `Bad Request: can't parse entities: Unsupported start tag "x" at byte offset 1`)
		}
		return okReply(map[string]any{"message_id": 7})
	})
	if _, err := h.gw.Send(h.ctx, domain.Outgoing{ThreadID: 42, Text: "<x>\n" + lines(24), Markdown: true, Fold: 20, Footer: "⏱ 2 min"}); err != nil {
		t.Fatal(err)
	}
	calls := h.api.callsOf("sendMessage")
	if len(calls) != 2 {
		t.Fatalf("calls = %d", len(calls))
	}
	if got := calls[1].form.Get("text"); !strings.HasPrefix(got, "<blockquote expandable><pre>&lt;x&gt;\n") || !strings.HasSuffix(got, "</pre></blockquote>\n⏱ 2 min") {
		t.Errorf("fallback = %q", got)
	}
}
