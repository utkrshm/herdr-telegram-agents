package system

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

func TestOpenCodeShellPreflightLogUsesFixedReasonWithoutSensitiveDetails(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	shell := &OpenCodeShell{}
	shell.logPreflightFailure("command-baseline-unavailable")
	got := output.String()
	if !strings.Contains(got, `"reason":"command-baseline-unavailable"`) {
		t.Fatalf("fixed preflight reason missing from log: %s", got)
	}
	for _, forbidden := range []string{"ses_private", "/private/path", "secret payload"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("sensitive or unrelated detail %q in log: %s", forbidden, got)
		}
	}
}

// Sanitized lower-screen captures from a disposable OpenCode 1.19.6 TUI.
const (
	openCodeNormalComposerFixture = "┃\n┃ Build auto · GPT-6.1 · provider\n╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀\n                                                   tab agents  ctrl+p commands"
	openCodeShellComposerFixture  = "┃\n┃ Run a command…\n┃\n┃ Shell\n╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀\n                                                           esc exit shell mode"
	// The production pane's visible screen has prior conversation above the
	// composer, a blank shell composer row, and a footer aligned far right.
	openCodeProductionShellScreen = `  ┃ prior conversation mentioning Shell and Run a command…
  ┃
  ┃
  ┃ Shell
  ╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
     /home/utkarsh/project                                                        esc exit shell mode    • OpenCode 1.19.6`
)

func openCodeShellDraftFixture(command string) string {
	return "┃ " + command + "\n┃ Shell\n╹━━━━━━━━━━━━━━━━━━━━\n/repo · main"
}

// openCodeScreenFixture models the captured 1.19.6 styling: transcript is
// #141414, the active composer is #1e1e1e, and surrounding UI is #0a0a0a.
func openCodeScreenFixture(text string) domain.Screen {
	lines := strings.Split(text, "\n")
	border := -1
	for index := range lines {
		if strings.Contains(lines[index], "╹") {
			border = index
		}
	}
	var ansi strings.Builder
	for index, line := range lines {
		background := "20;20;20"
		if border >= 2 && (index == border || index == border-1 || index == border-2) {
			background = "30;30;30"
		}
		ansi.WriteString("\x1b[48;2;")
		ansi.WriteString(background)
		ansi.WriteByte('m')
		ansi.WriteString(line)
		ansi.WriteString("\x1b[0m")
		if index+1 < len(lines) {
			ansi.WriteByte('\n')
		}
	}
	return domain.Screen{Text: text, ANSI: ansi.String()}
}

func TestCorrelateOpenCodeCommandRequiresUniqueNewSyntheticTool(t *testing.T) {
	part := func(id, command, status, output string) OpenCodeCommandPart {
		return OpenCodeCommandPart{ID: id, Type: "tool", Tool: "bash", Command: command, Status: status, Output: output, OutputPresent: true}
	}
	message := func(p ...OpenCodeCommandPart) OpenCodeCommandMessage {
		return OpenCodeCommandMessage{Role: "assistant", ParentID: "fresh", ParentText: openCodeCommandMarker, Synthetic: true, ParentKnown: true, Parts: p}
	}
	base := OpenCodeCommandSnapshot{"old": {}}
	if result, done, err := CorrelateOpenCodeCommand(base, []OpenCodeCommandMessage{message(part("old", "pwd", "completed", "old"))}, "pwd"); err != nil || done {
		t.Fatalf("old record accepted: %+v %v %v", result, done, err)
	}
	if _, done, err := CorrelateOpenCodeCommand(base, []OpenCodeCommandMessage{message(part("new", "pwd", "running", ""))}, "pwd"); err != nil || done {
		t.Fatalf("running record: done=%v err=%v", done, err)
	}
	result, done, err := CorrelateOpenCodeCommand(base, []OpenCodeCommandMessage{message(part("new", "pwd", "completed", "/repo"))}, "pwd")
	if err != nil || !done || result.Output != "/repo" || result.ExitStatus != nil {
		t.Fatalf("completion=%+v done=%v err=%v", result, done, err)
	}
	if _, _, err = CorrelateOpenCodeCommand(base, []OpenCodeCommandMessage{message(part("a", "pwd", "completed", ""), part("b", "pwd", "completed", ""))}, "pwd"); err == nil {
		t.Fatal("ambiguous command records accepted")
	}
	otherParent := message(part("another", "other", "completed", ""))
	otherParent.ParentID = "other-parent"
	if _, _, err = CorrelateOpenCodeCommand(base, []OpenCodeCommandMessage{message(part("one", "pwd", "completed", "")), otherParent}, "pwd"); err == nil {
		t.Fatal("multiple new synthetic parents accepted")
	}
	if _, done, _ = CorrelateOpenCodeCommand(base, []OpenCodeCommandMessage{{Role: "assistant", Parts: []OpenCodeCommandPart{part("new", "pwd", "completed", "x")}}}, "pwd"); done {
		t.Fatal("record without synthetic parent accepted")
	}
	notSynthetic := message(part("new-synth-flag", "pwd", "completed", "x"))
	notSynthetic.Synthetic = false
	if _, done, _ := CorrelateOpenCodeCommand(base, []OpenCodeCommandMessage{notSynthetic}, "pwd"); done {
		t.Fatal("parent without synthetic=true accepted")
	}
	base["user:old-parent"] = struct{}{}
	staleParent := message(part("fresh-part", "pwd", "completed", "wrong"))
	staleParent.ParentID = "old-parent"
	if _, done, _ := CorrelateOpenCodeCommand(base, []OpenCodeCommandMessage{staleParent}, "pwd"); done {
		t.Fatal("baseline parent id was attributed to a new command")
	}
}

func TestCommandOperationMarkerIsUniqueAndFailClosedForAmbiguousSyntax(t *testing.T) {
	command := `printf 'value # literal'`
	first, err := commandWithOperationID(command, "request-one")
	if err != nil || first != command+" # herdr-tg-op:request-one" {
		t.Fatalf("marked command=%q err=%v", first, err)
	}
	second, err := commandWithOperationID(command, "request-two")
	if err != nil || second == first {
		t.Fatalf("markers are not unique: %q %q err=%v", first, second, err)
	}
	for _, input := range []string{"echo unfinished'", "echo escaped\\", `printf "%s" foo#bar\`, "echo # comment", "cat <<EOF\nEOF", "echo one\necho two"} {
		if _, err := commandWithOperationID(input, "request"); err == nil {
			t.Errorf("unsafe command accepted: %q", input)
		}
	}
}

func TestOpenCodeCorrelationIgnoresManualSameCommand(t *testing.T) {
	marked, err := commandWithOperationID("pwd", "only-this-request")
	if err != nil {
		t.Fatal(err)
	}
	manual := OpenCodeCommandMessage{Role: "assistant", ParentID: "new-parent", ParentText: openCodeCommandMarker, Synthetic: true, ParentKnown: true, Parts: []OpenCodeCommandPart{{ID: "manual", Type: "tool", Tool: "bash", Command: "pwd", Status: "completed", Output: "manual", OutputPresent: true}}}
	if _, complete, err := CorrelateOpenCodeCommand(nil, []OpenCodeCommandMessage{manual}, marked); err == nil || complete {
		t.Fatalf("unmarked command was not rejected as unverifiable: complete=%v err=%v", complete, err)
	}
	manual.Parts[0].Command = marked
	manual.Parts[0].Output = "tagged"
	result, complete, err := CorrelateOpenCodeCommand(nil, []OpenCodeCommandMessage{manual}, marked)
	if err != nil || !complete || result.Output != "tagged" {
		t.Fatalf("tagged result=%+v complete=%v err=%v", result, complete, err)
	}
}

func TestOpenCodeTUIRequiresRealEmptyComposerMetadataBorderAndFooter(t *testing.T) {
	if openCodeInputIsSafe(openCodeProductionShellScreen, true) {
		t.Fatal("plain-text production screen with transcript was accepted without a provable composer boundary")
	}
	if openCodeInputIsSafe(openCodeProductionShellScreen, false) {
		t.Fatal("already-active production shell composer was mistaken for normal input")
	}
	normal := `older transcript says Shell and Run a command…
┃
┃
┃ Build auto · GPT-6.1 · provider
╹━━━━━━━━━━━━━━━━━━━━
/repo  ·  main`
	shell := `old transcript mentions Shell
┃
┃
┃ Shell
╹━━━━━━━━━━━━━━━━━━━━
/repo  ·  main`
	if openCodeInputIsSafe(normal, false) || openCodeInputIsSafe(normal, true) {
		t.Fatal("normal composer adjacent to transcript accepted without a verified boundary")
	}
	if openCodeInputIsSafe(shell, true) || openCodeInputIsSafe(shell, false) {
		t.Fatal("shell composer adjacent to transcript accepted without a verified boundary")
	}
	withoutPlaceholder := "┃\n┃\n┃\n┃ Shell\n╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀\n/repo · main"
	if !openCodeInputIsSafe(withoutPlaceholder, true) {
		t.Fatal("real blank shell composer without a placeholder was refused")
	}
	for name, screen := range map[string]string{
		"draft":         strings.Replace(shell, "┃\n┃\n┃ Shell", "┃\n┃ rm -rf /\n┃ Shell", 1),
		"no footer":     strings.TrimSuffix(shell, "/repo  ·  main"),
		"no box border": strings.Replace(shell, "╹━━━━━━━━━━━━━━━━━━━━", "footer", 1),
		"shell label from transcript": `transcript Shell
┃ Run a command… "hint"
┃ Build
╹━━━━━━━━
/repo`,
		"modal": `Choose
1. Continue
2. Cancel
┃
┃
┃ Shell
╹━━━━━━━━
/repo · Enter to select · Esc to cancel`,
	} {
		t.Run(name, func(t *testing.T) {
			if openCodeInputIsSafe(screen, true) {
				t.Fatal("unsafe screen accepted")
			}
		})
	}
}

func TestOpenCodeShellDraftUsesComposerGeometryAndExactPlaceholder(t *testing.T) {
	command := "pwd # herdr-tg-op:fixture"
	productionGeometry := "┃ " + command + "\n┃ Shell    ~/utkarsh-namma-space-iitb:master\n╹━━━━━━━━━━━━━━━━━━━━\n/repo · main"
	if !openCodeShellDraftMatches(productionGeometry, command) {
		t.Fatal("Shell label with right-aligned cwd/branch was not recognized")
	}
	for name, screen := range map[string]string{
		"trailing blank composer row": "┃ " + command + "\n┃\n┃ Shell    ~/project:master\n╹━━━━━━━━━━━━━━━━━━━━\n/repo · main",
		"placeholder prefix is draft": "┃ Run a command… extra\n┃ Shell    ~/project:master\n╹━━━━━━━━━━━━━━━━━━━━\n/repo · main",
		"modal after SendText":        "┃ " + command + "\n┃ Shell    ~/project:master\n╹━━━━━━━━━━━━━━━━━━━━\n1. Continue\n2. Cancel\nEnter to select · Esc to cancel",
	} {
		t.Run(name, func(t *testing.T) {
			if openCodeShellDraftMatches(screen, command) {
				t.Fatal("unsafe or non-exact draft accepted")
			}
		})
	}
}

func TestOpenCodeANSIComposerRequiresStyledActiveRegion(t *testing.T) {
	styled := func(text string, color string) string { return "\x1b[48;2;" + color + "m" + text + "\x1b[0m" }
	transcript := styled("┃ transcript mentioning Shell and command text", "20;20;20")
	blank := styled("┃", "30;30;30")
	metadata := styled("┃ Shell  ~/project", "30;30;30")
	border := styled("╹▀▀▀▀▀", "30;30;30")
	footer := styled("esc exit shell mode", "10;10;10")
	ready := strings.Join([]string{transcript, blank, metadata, border, footer}, "\n")
	if got := openCodeANSIComposerMode(ready); got != "shell" {
		t.Fatalf("ANSI shell mode = %q rows=%+v", got, openCodeStyledRows(ready))
	}
	if !openCodeInputIsSafeScreen(domain.Screen{Text: stripOpenCodeANSI(ready), ANSI: ready}, true) {
		t.Fatal("styled, empty shell composer was not accepted")
	}
	if openCodeInputIsSafeScreen(domain.Screen{Text: stripOpenCodeANSI(ready)}, true) {
		t.Fatal("plain text must not be sufficient to authorize shell input")
	}
	metadataOnly := strings.Join([]string{transcript, styled("┃", "20;20;20"), metadata, border, footer}, "\n")
	if openCodeInputIsSafeScreen(domain.Screen{Text: stripOpenCodeANSI(metadataOnly), ANSI: metadataOnly}, true) {
		t.Fatal("styled metadata without a positively styled input row was accepted")
	}
	uncoloredDraft := strings.Join([]string{transcript, styled("┃ pwd # herdr-tg-op:test", "20;20;20"), metadata, border, footer}, "\n")
	if openCodeInputIsSafeScreen(domain.Screen{Text: stripOpenCodeANSI(uncoloredDraft), ANSI: uncoloredDraft}, true) {
		t.Fatal("draft whose row lacks composer styling was accepted")
	}
	draft := strings.Join([]string{transcript, styled("┃ pwd # herdr-tg-op:test", "30;30;30"), metadata, border, footer}, "\n")
	if !openCodeShellDraftMatchesANSI(draft, "pwd # herdr-tg-op:test") {
		t.Fatal("exact draft in styled composer was not recognized")
	}
	if openCodeShellDraftMatchesANSI(strings.Join([]string{styled("┃ pwd # herdr-tg-op:test", "20;20;20"), metadata, border, footer}, "\n"), "pwd # herdr-tg-op:test") {
		t.Fatal("transcript-like draft was mistaken for composer input")
	}
	modal := ready + "\n" + styled("Enter to select   Esc to cancel", "45;31;38")
	if openCodeInputIsSafeScreen(domain.Screen{Text: stripOpenCodeANSI(modal), ANSI: modal}, true) {
		t.Fatal("modal screen must not be accepted")
	}
}

func TestOpenCodeInputRejectsDraftAdjacentToBlankAndRecognizesTranscriptBoundary(t *testing.T) {
	for _, test := range []struct {
		name  string
		shell bool
		text  string
		want  bool
	}{
		{"normal draft then blank", false, "┃ draft text\n┃\n┃ Build auto · provider\n╹━━━━━━━━\n/repo", false},
		{"shell draft then blank", true, "┃ draft text\n┃\n┃ Shell    ~/project:master\n╹━━━━━━━━\n/repo", false},
		{"normal blank composer after transcript", false, "┃ prior transcript text\n┃\n┃\n┃ Build auto · provider\n╹━━━━━━━━\n/repo", false},
		{"shell blank composer after transcript", true, "┃ prior transcript text\n┃\n┃\n┃ Shell    ~/project:master\n╹━━━━━━━━\n/repo", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := openCodeInputIsSafe(test.text, test.shell); got != test.want {
				t.Fatalf("openCodeInputIsSafe()=%v, want %v", got, test.want)
			}
		})
	}
}

func TestOpenCodeCommandReaderAgainstSQLiteFixture(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	db := filepath.Join(t.TempDir(), "opencode.db")
	script := `import sqlite3,sys
c=sqlite3.connect(sys.argv[1])
c.executescript('CREATE TABLE message(id TEXT, session_id TEXT, data TEXT); CREATE TABLE part(id TEXT, message_id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);')
c.execute('INSERT INTO message VALUES(?,?,?)',('u','ses_real','{"role":"user","time":{"created":100}}'))
c.execute('INSERT INTO message VALUES(?,?,?)',('a','ses_real','{"role":"assistant","parentID":"u","time":{"created":101}}'))
c.execute('INSERT INTO part VALUES(?,?,?,?,?,?)',('up','u','ses_real',100,100,'{"type":"text","synthetic":true,"text":"The following tool was executed by the user"}'))
c.execute('INSERT INTO part VALUES(?,?,?,?,?,?)',('p','a','ses_real',101,101,'{"type":"tool","tool":"bash","state":{"status":"completed","input":{"command":"pwd"},"output":"/tmp"}}'))
c.commit();c.close()`
	if output, err := exec.Command(python, "-c", script, db).CombinedOutput(); err != nil {
		t.Fatalf("create DB fixture: %v: %s", err, output)
	}
	binDir := t.TempDir()
	if err := os.Symlink(python, filepath.Join(binDir, "python3")); err != nil {
		t.Skipf("cannot make isolated PATH fixture: %v", err)
	}
	t.Setenv("PATH", binDir) // prove the sqlite3-missing fallback uses Python stdlib.
	if _, err := runOpenCodeSQLite(context.Background(), db, `SELECT id FROM part LIMIT 1`); err != nil {
		t.Fatalf("read-only Python SQLite fallback: %v", err)
	}
	query := func(ctx context.Context, path, sql string) ([]byte, error) {
		return runOpenCodeSQLite(ctx, path, sql)
	}
	baseline, err := snapshotOpenCodeCommand(context.Background(), query, db, "ses_real")
	if err != nil {
		t.Fatal(err)
	}
	if len(baseline.ids) != 3 {
		t.Fatalf("baseline IDs=%v", baseline.ids)
	}
	messages, err := readOpenCodeCommandMessages(context.Background(), query, db, "ses_real", 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, done, _ := CorrelateOpenCodeCommand(baseline.ids, messages, "pwd"); done {
		t.Fatal("baseline synthetic parent was attributed to a new command")
	}
	beforeBoundary, err := readOpenCodeCommandMessages(context.Background(), query, db, "ses_real", 101)
	if err != nil || len(beforeBoundary) != 0 {
		t.Fatalf("pre-boundary parent leaked into candidates: %v %v", beforeBoundary, err)
	}
	// The current record is in the baseline, so correlate against an empty
	// pre-command snapshot to exercise the exact SQLite TEXT serialization.
	result, done, err := CorrelateOpenCodeCommand(OpenCodeCommandSnapshot{}, messages, "pwd")
	if err != nil || !done || result.Output != "/tmp" || !result.OutputPresent {
		t.Fatalf("result=%+v done=%v err=%v", result, done, err)
	}
	if _, err := os.Stat(db); err != nil {
		t.Fatal(err)
	}
}

func TestOpenCodeCommandReaderStopsBeforeFetchingOversizedToolPayload(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	db := filepath.Join(t.TempDir(), "oversize.db")
	script := `import sqlite3,sys,json
c=sqlite3.connect(sys.argv[1])
c.executescript('CREATE TABLE message(id TEXT,session_id TEXT,data TEXT); CREATE TABLE part(id TEXT,message_id TEXT,session_id TEXT,time_created INTEGER,time_updated INTEGER,data TEXT);')
c.execute('INSERT INTO message VALUES(?,?,?)',('u','ses_big','{"role":"user"}'))
c.execute('INSERT INTO message VALUES(?,?,?)',('a','ses_big','{"role":"assistant","parentID":"u"}'))
c.execute('INSERT INTO part VALUES(?,?,?,?,?,?)',('up','u','ses_big',100,100,json.dumps({"type":"text","synthetic":True,"text":"The following tool was executed by the user"})))
c.execute('INSERT INTO part VALUES(?,?,?,?,?,?)',('big','a','ses_big',101,101,'x'*(8*1024*1024)))
c.commit();c.close()`
	if out, err := exec.Command(python, "-c", script, db).CombinedOutput(); err != nil {
		t.Fatalf("create oversized fixture: %v %s", err, out)
	}
	query := func(ctx context.Context, path, sql string) ([]byte, error) {
		return runOpenCodePythonSQLite(ctx, path, sql)
	}
	_, err = readOpenCodeCommandMessages(context.Background(), query, db, "ses_big", 100)
	if !errors.Is(err, errOpenCodeDBOutputLimit) {
		t.Fatalf("oversized row error=%v", err)
	}
}

func TestReadOpenCodeCommandMessagesParsesSQLiteJSONTextAndParentID(t *testing.T) {
	rows, marshalErr := json.Marshal([]map[string]string{{"message_id": "a", "message_data": `{"role":"assistant","parentID":"u","time":{"created":101}}`, "parent_data": `{"role":"user"}`, "parent_part_data": `{"type":"text","synthetic":false,"text":"The following tool was executed by the user"}`, "part_id": "p1", "part_data": `{"type":"tool","tool":"bash","state":{"status":"completed","input":{"command":"pwd"},"output":"/repo"}}`}})
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	reader := func(_ context.Context, _, query string) ([]byte, error) {
		if !strings.Contains(query, "parent.id=json_extract") || !strings.Contains(query, "LIMIT 501") || !strings.Contains(query, ">=100") || strings.Contains(query, "p.type") {
			t.Fatalf("query not bounded/incremental: %s", query)
		}
		return rows, nil
		return []byte(`[{"message_id":"a","message_data":"{\"role\":\"assistant\",\"parentID\":\"u\",\"time\":{\"created\":101}}","parent_data":"{\"role\":\"user\",\"parts\":[{\"type\":\"text\",\"text\":\"The following tool was executed by the user\"}]}","part_id":"p1","part_data":"{\"type\":\"tool\",\"tool\":\"bash\",\"state\":{\"status\":\"completed\",\"input\":{\"command\":\"pwd\"},\"output\":\"/repo\"}}"}]`), nil
	}
	messages, err := readOpenCodeCommandMessages(context.Background(), reader, "db", "ses_1", 100)
	if err != nil || len(messages) != 1 || messages[0].Role != "assistant" || messages[0].Synthetic || !strings.Contains(messages[0].ParentText, openCodeCommandMarker) || len(messages[0].Parts) != 1 {
		t.Fatalf("messages=%+v err=%v", messages, err)
	}
	part := messages[0].Parts[0]
	if part.Tool != "bash" || part.Command != "pwd" || part.Status != "completed" || part.Output != "/repo" || !part.OutputPresent {
		t.Fatalf("part=%+v", part)
	}
}

func TestOpenCodeShellUsesAlreadyActiveShellWithoutTogglingMode(t *testing.T) {
	t.Run("normal composer transitions through shell mode exactly once", func(t *testing.T) {
		testOpenCodeShellSubmissionFlow(t, false)
	})
	t.Run("already active shell does not toggle mode", func(t *testing.T) {
		testOpenCodeShellSubmissionFlow(t, true)
	})
}

func testOpenCodeShellSubmissionFlow(t *testing.T, alreadyShell bool) {
	t.Helper()
	tuple := domain.SessionTuple{Source: "herdr:opencode", Agent: domain.OpenCodeKind, Kind: "id", Value: "ses_1"}
	var events []string
	var reads atomic.Int32
	var enteredCommand string
	if !openCodeShellDraftMatches(openCodeShellDraftFixture("pwd # herdr-tg-op:request-42"), "pwd # herdr-tg-op:request-42") {
		t.Fatal("exact draft fixture rejected")
	}
	submitted := false
	var progressCalls atomic.Int32
	shell := &OpenCodeShell{Session: func(context.Context, string) (domain.SessionTuple, error) { return tuple, nil }, DBPath: func() (string, error) { return "db", nil }, Poll: time.Millisecond, OperationID: func() string { return "request-42" },
		ReadScreen: func(context.Context, string) (domain.Screen, error) {
			if reads.Add(1) == 1 {
				if alreadyShell {
					return openCodeScreenFixture(openCodeShellComposerFixture), nil
				}
				return openCodeScreenFixture(openCodeNormalComposerFixture), nil
			}
			if !alreadyShell && reads.Load() == 2 {
				return openCodeScreenFixture(openCodeShellComposerFixture), nil
			}
			wantDraftRead := int32(2)
			if !alreadyShell {
				wantDraftRead = 3
			}
			if reads.Load() == wantDraftRead {
				return openCodeScreenFixture(openCodeShellDraftFixture(enteredCommand)), nil
			}
			return domain.Screen{Text: "Shell running"}, nil
		},
		SendKeys: func(_ context.Context, _ string, keys []string) error {
			events = append(events, "key:"+strings.Join(keys, "+"))
			return nil
		},
		SendText: func(_ context.Context, _, text string) error {
			enteredCommand = text
			events = append(events, "text:"+text)
			return nil
		},
		SQLite: func(_ context.Context, _, query string) ([]byte, error) {
			switch {
			case strings.Contains(query, "SELECT id FROM"):
				return []byte(`[{"id":"old"}]`), nil
			case strings.Contains(query, "LIKE"):
				return []byte(`[]`), nil
			default:
				created := time.Now().Add(time.Second).UnixMilli()
				row := map[string]any{"message_id": "m", "message_data": fmt.Sprintf(`{"role":"assistant","parentID":"u","time":{"created":%d}}`, created), "parent_data": `{"role":"user"}`, "parent_part_data": `{"type":"text","synthetic":true,"text":"The following tool was executed by the user"}`, "part_id": "new", "part_data": fmt.Sprintf(`{"type":"tool","tool":"bash","state":{"status":"completed","input":{"command":%q},"output":"/repo"}}`, enteredCommand)}
				return json.Marshal([]any{row})
			}
		},
	}
	agent := domain.Agent{Key: domain.Key{PaneID: "pane", SessionDigest: tuple.Digest()}, Kind: domain.OpenCodeKind}
	shell.ProgressAfter = 5 * time.Second
	got, err := shell.Run(context.Background(), agent, "pwd", func() error { submitted = true; return nil }, func(context.Context, domain.Screen) error {
		progressCalls.Add(1)
		return nil
	})
	if err != nil || got != "/repo" || !submitted {
		t.Fatalf("Run=%q err=%v submitted=%v", got, err, submitted)
	}
	if progressCalls.Load() != 0 {
		t.Fatalf("fast completion sent %d progress snapshots before the deadline", progressCalls.Load())
	}
	want := []string{"text:pwd # herdr-tg-op:request-42", "key:enter"}
	if !alreadyShell {
		want = append([]string{"key:!"}, want...)
	}
	if strings.Join(events, "|") != strings.Join(want, "|") {
		t.Fatalf("events=%v want=%v", events, want)
	}
	if got := strings.Count(strings.Join(events, "|"), "key:!"); got != map[bool]int{true: 0, false: 1}[alreadyShell] {
		t.Fatalf("shell-mode key sent %d times (alreadyShell=%v), events=%v", got, alreadyShell, events)
	}
}

func TestOpenCodeShellRefusesUnverifiableInputAndDoesNotResend(t *testing.T) {
	tuple := domain.SessionTuple{Source: "herdr:opencode", Agent: domain.OpenCodeKind, Kind: "id", Value: "ses_1"}
	var keys atomic.Int32
	shell := &OpenCodeShell{Session: func(context.Context, string) (domain.SessionTuple, error) { return tuple, nil }, DBPath: func() (string, error) { return "db", nil }, SQLite: func(_ context.Context, _, query string) ([]byte, error) {
		if strings.Contains(query, "SELECT id FROM") {
			return []byte(`[]`), nil
		}
		if strings.Contains(query, "LIKE") {
			return []byte(`[]`), nil
		}
		return []byte(`[{"created_at":1}]`), nil
	}, ReadScreen: func(context.Context, string) (domain.Screen, error) {
		return domain.Screen{Text: "OpenCode\n❯ old text"}, nil
	}, SendKeys: func(context.Context, string, []string) error { keys.Add(1); return nil }, SendText: func(context.Context, string, string) error { return nil }}
	_, err := shell.Run(context.Background(), domain.Agent{Key: domain.Key{PaneID: "p", SessionDigest: tuple.Digest()}, Kind: domain.OpenCodeKind}, "pwd", nil, nil)
	if err == nil || keys.Load() != 0 {
		t.Fatalf("err=%v key events=%d", err, keys.Load())
	}
}

func TestOpenCodeShellRequiresBoundedModeTransitionAndVisibleDraft(t *testing.T) {
	for _, test := range []struct {
		name             string
		shellModeVisible bool
		draftMatches     bool
		wantKeyCount     int
		wantText         bool
		wantStage        domain.ShellStage
	}{{"mode timeout", false, false, 1, false, domain.ShellStageShellModeCheck}, {"draft mismatch", true, false, 1, true, domain.ShellStageDraftVerification}} {
		t.Run(test.name, func(t *testing.T) {
			tuple := domain.SessionTuple{Source: "herdr:opencode", Agent: domain.OpenCodeKind, Kind: "id", Value: "ses_1"}
			var reads atomic.Int32
			var keys []string
			var entered string
			shell := &OpenCodeShell{
				Session:        func(context.Context, string) (domain.SessionTuple, error) { return tuple, nil },
				DBPath:         func() (string, error) { return "db", nil },
				SQLite:         func(context.Context, string, string) ([]byte, error) { return []byte(`[]`), nil },
				TransitionWait: 5 * time.Millisecond, TransitionPoll: time.Millisecond, OperationID: func() string { return "bounded" },
				ReadScreen: func(context.Context, string) (domain.Screen, error) {
					n := reads.Add(1)
					if n == 1 {
						return openCodeScreenFixture(openCodeNormalComposerFixture), nil
					}
					if test.shellModeVisible && n == 2 {
						return openCodeScreenFixture(openCodeShellComposerFixture), nil
					}
					if test.shellModeVisible && n >= 3 {
						value := "not the submitted command"
						if test.draftMatches {
							value = "pwd # herdr-tg-op:bounded"
						}
						return openCodeScreenFixture(openCodeShellDraftFixture(value)), nil
					}
					return openCodeScreenFixture(openCodeNormalComposerFixture), nil
				},
				SendKeys: func(_ context.Context, _ string, events []string) error { keys = append(keys, events...); return nil },
				SendText: func(_ context.Context, _, value string) error { entered = value; return nil },
			}
			_, err := shell.Run(context.Background(), domain.Agent{Key: domain.Key{PaneID: "p", SessionDigest: tuple.Digest()}, Kind: domain.OpenCodeKind}, "pwd", nil, nil)
			if err == nil || len(keys) != test.wantKeyCount || strings.Contains(strings.Join(keys, "|"), "enter") || (entered != "") != test.wantText {
				t.Fatalf("err=%v keys=%v text=%q", err, keys, entered)
			}
			if stage, ok := domain.ShellErrorStage(err); !ok || stage != test.wantStage {
				t.Fatalf("stage = %q, %v; want %q", stage, ok, test.wantStage)
			}
		})
	}
}

func TestOpenCodeShellSessionChangePreventsSubmit(t *testing.T) {
	tuple := domain.SessionTuple{Source: "herdr:opencode", Agent: domain.OpenCodeKind, Kind: "id", Value: "ses_1"}
	var calls atomic.Int32
	shell := &OpenCodeShell{Session: func(context.Context, string) (domain.SessionTuple, error) {
		if calls.Add(1) > 2 {
			tuple.Value = "ses_2"
		}
		return tuple, nil
	}, DBPath: func() (string, error) { return "db", nil }, SQLite: func(_ context.Context, _, query string) ([]byte, error) {
		if strings.Contains(query, "SELECT id FROM") {
			return []byte(`[]`), nil
		}
		if strings.Contains(query, "LIKE") {
			return []byte(`[]`), nil
		}
		return []byte(`[{"created_at":1}]`), nil
	}, ReadScreen: func(context.Context, string) (domain.Screen, error) {
		return openCodeScreenFixture(openCodeNormalComposerFixture), nil
	}, SendKeys: func(context.Context, string, []string) error { return nil }, SendText: func(context.Context, string, string) error { return nil }}
	_, err := shell.Run(context.Background(), domain.Agent{Key: domain.Key{PaneID: "p", SessionDigest: tuple.Digest()}, Kind: domain.OpenCodeKind}, "pwd", nil, nil)
	if err == nil {
		t.Fatal("session switch accepted")
	}
}

func TestOpenCodeShellWaitIsCancelableWithoutResubmit(t *testing.T) {
	tuple := domain.SessionTuple{Source: "herdr:opencode", Agent: domain.OpenCodeKind, Kind: "id", Value: "ses_1"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var submits atomic.Int32
	var screens atomic.Int32
	var entered string
	accepted := make(chan struct{})
	progressSeen := make(chan struct{})
	shell := &OpenCodeShell{Session: func(context.Context, string) (domain.SessionTuple, error) { return tuple, nil }, DBPath: func() (string, error) { return "db", nil }, Poll: time.Millisecond, ProgressAfter: time.Millisecond, OperationID: func() string { return "cancel-test" }, ReadScreen: func(_ context.Context, _ string) (domain.Screen, error) {
		if screens.Add(1) == 1 {
			return openCodeScreenFixture(openCodeNormalComposerFixture), nil
		}
		if screens.Load() == 2 {
			return openCodeScreenFixture(openCodeShellComposerFixture), nil
		}
		if screens.Load() == 3 {
			return openCodeScreenFixture(openCodeShellDraftFixture(entered)), nil
		}
		return openCodeScreenFixture(openCodeShellComposerFixture), nil
	}, SendKeys: func(context.Context, string, []string) error { submits.Add(1); return nil }, SendText: func(_ context.Context, _, text string) error { entered = text; return nil }, SQLite: func(_ context.Context, _, query string) ([]byte, error) {
		if strings.Contains(query, "SELECT id FROM") {
			return []byte(`[]`), nil
		}
		if strings.Contains(query, "LIKE") {
			return []byte(`[]`), nil
		}
		return []byte(`[{"created_at":1}]`), nil
	}}
	done := make(chan error, 1)
	go func() {
		_, err := shell.Run(ctx, domain.Agent{Key: domain.Key{PaneID: "p", SessionDigest: tuple.Digest()}, Kind: domain.OpenCodeKind}, "pwd", func() error { close(accepted); return nil }, func(_ context.Context, screen domain.Screen) error {
			if !strings.Contains(screen.Text, "Shell") {
				t.Errorf("progress screen=%q", screen.Text)
			}
			close(progressSeen)
			return nil
		})
		done <- err
	}()
	<-accepted
	<-progressSeen
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	} else if stage, ok := domain.ShellErrorStage(err); !ok || (stage != domain.ShellStageRecordWait && stage != domain.ShellStageRecordRead) {
		t.Fatalf("record watcher stage = %q, %v", stage, ok)
	}
	if submits.Load() != 2 {
		t.Fatalf("key calls=%d", submits.Load())
	}
}

func TestOpenCodeProgressSnapshotIsIndependentOfStalledDatabaseProbe(t *testing.T) {
	tuple := domain.SessionTuple{Source: "herdr:opencode", Agent: domain.OpenCodeKind, Kind: "id", Value: "ses_slow"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accepted := make(chan struct{})
	progressed := make(chan struct{})
	var screenRead atomic.Int32
	var keys atomic.Int32
	shell := &OpenCodeShell{Session: func(context.Context, string) (domain.SessionTuple, error) { return tuple, nil }, DBPath: func() (string, error) { return "db", nil }, Poll: time.Millisecond, ProbeTimeout: time.Second, ProgressAfter: 20 * time.Millisecond, ScreenProbeTimeout: 100 * time.Millisecond, OperationID: func() string { return "stalled" },
		ReadScreen: func(context.Context, string) (domain.Screen, error) {
			n := screenRead.Add(1)
			switch n {
			case 1:
				return openCodeScreenFixture(openCodeNormalComposerFixture), nil
			case 2:
				return openCodeScreenFixture(openCodeShellComposerFixture), nil
			case 3:
				return openCodeScreenFixture(openCodeShellDraftFixture("pwd # herdr-tg-op:stalled")), nil
			default:
				return openCodeScreenFixture(openCodeShellComposerFixture), nil
			}
		},
		SendKeys: func(context.Context, string, []string) error { keys.Add(1); return nil }, SendText: func(context.Context, string, string) error { return nil },
		SQLite: func(probe context.Context, _, query string) ([]byte, error) {
			if strings.Contains(query, "SELECT id FROM") || strings.Contains(query, "LIKE") {
				return []byte(`[]`), nil
			}
			<-probe.Done()
			return nil, probe.Err()
		},
	}
	finished := make(chan error, 1)
	go func() {
		_, err := shell.Run(ctx, domain.Agent{Key: domain.Key{PaneID: "p", SessionDigest: tuple.Digest()}, Kind: domain.OpenCodeKind}, "pwd", func() error { close(accepted); return nil }, func(_ context.Context, snapshot domain.Screen) error {
			if !strings.Contains(snapshot.Text, "shell command has no verified result yet") {
				t.Errorf("snapshot=%q", snapshot.Text)
			}
			close(progressed)
			cancel()
			return nil
		})
		finished <- err
	}()
	<-accepted
	select {
	case <-progressed:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("progress snapshot waited behind stalled database probe")
	}
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("watcher error=%v", err)
	} else if stage, ok := domain.ShellErrorStage(err); !ok || stage != domain.ShellStageRecordRead {
		t.Fatalf("database-read stage = %q, %v", stage, ok)
	}
	if keys.Load() != 2 {
		t.Fatalf("key events=%d, want ! and enter only", keys.Load())
	}
}

func TestOpenCodeProgressSnapshotDoesNotRaceCompletedResult(t *testing.T) {
	started := make(chan struct{})
	finishRead := make(chan struct{})
	progressed := make(chan struct{}, 1)
	state := &openCodeProgressState{}
	shell := &OpenCodeShell{
		ProgressAfter: 5 * time.Millisecond,
		ReadScreen: func(context.Context, string) (domain.Screen, error) {
			close(started)
			<-finishRead
			return domain.Screen{Text: "stale snapshot"}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shell.startProgressSnapshot(ctx, state, "pane", func(context.Context, domain.Screen) error {
		progressed <- struct{}{}
		return nil
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("snapshot read did not start")
	}
	state.mu.Lock()
	state.completed = true
	state.mu.Unlock()
	close(finishRead)
	select {
	case <-progressed:
		t.Fatal("stale snapshot was sent after completion")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestOpenCodeRunCancelsBlockedProgressWhenResultCompletes(t *testing.T) {
	tuple := domain.SessionTuple{Source: "herdr:opencode", Agent: domain.OpenCodeKind, Kind: "id", Value: "ses_progress_cancel"}
	progressStarted := make(chan struct{})
	var screens atomic.Int32
	var submittedCommand string
	var resultReads atomic.Int32
	shell := &OpenCodeShell{
		Session: func(context.Context, string) (domain.SessionTuple, error) { return tuple, nil },
		DBPath:  func() (string, error) { return "db", nil },
		Poll:    time.Millisecond, ProgressAfter: 5 * time.Millisecond,
		OperationID: func() string { return "progress-cancel" },
		ReadScreen: func(context.Context, string) (domain.Screen, error) {
			switch screens.Add(1) {
			case 1:
				return openCodeScreenFixture(openCodeNormalComposerFixture), nil
			case 2:
				return openCodeScreenFixture(openCodeShellComposerFixture), nil
			case 3:
				return openCodeScreenFixture(openCodeShellDraftFixture(submittedCommand)), nil
			default:
				return domain.Screen{Text: "command running"}, nil
			}
		},
		SendKeys: func(context.Context, string, []string) error { return nil },
		SendText: func(_ context.Context, _, command string) error { submittedCommand = command; return nil },
		SQLite: func(_ context.Context, _, query string) ([]byte, error) {
			if strings.Contains(query, "SELECT id FROM") || strings.Contains(query, "LIKE") {
				return []byte(`[]`), nil
			}
			readNumber := resultReads.Add(1)
			status := "completed"
			if readNumber == 1 {
				status = "running"
			}
			if readNumber == 2 {
				<-progressStarted
			}
			part := fmt.Sprintf(`{"type":"tool","tool":"bash","state":{"status":%q,"input":{"command":%q},"output":"done"}}`, status, submittedCommand)
			row := map[string]any{"message_id": "m", "message_data": `{"role":"assistant","parentID":"u"}`, "parent_data": `{"role":"user"}`, "parent_part_data": `{"type":"text","synthetic":true,"text":"The following tool was executed by the user"}`, "part_id": "p", "part_data": part}
			return json.Marshal([]any{row})
		},
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		output, err := shell.Run(context.Background(), domain.Agent{Key: domain.Key{PaneID: "p", SessionDigest: tuple.Digest()}, Kind: domain.OpenCodeKind}, "pwd", nil, func(ctx context.Context, _ domain.Screen) error {
			close(progressStarted)
			<-ctx.Done()
			return ctx.Err()
		})
		if err != nil || output != "done" {
			t.Errorf("Run = %q, %v", output, err)
		}
	}()
	select {
	case <-progressStarted:
	case <-time.After(time.Second):
		t.Fatal("progress publication did not start")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Run blocked behind progress publication after result completed")
	}
}

func TestOpenCodeShellCancellationDuringDatabaseFailureKeepsRecordReadStage(t *testing.T) {
	tuple := domain.SessionTuple{Source: "herdr:opencode", Agent: domain.OpenCodeKind, Kind: "id", Value: "ses_cancel_db"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probeStarted := make(chan struct{})
	databaseReadFailed := make(chan struct{})
	var sessionCalls atomic.Int32
	var screenReads atomic.Int32
	shell := &OpenCodeShell{
		Session: func(callCtx context.Context, _ string) (domain.SessionTuple, error) {
			if sessionCalls.Add(1) >= 8 {
				select {
				case <-probeStarted:
				default:
					close(probeStarted)
				}
				<-callCtx.Done()
				return domain.SessionTuple{}, callCtx.Err()
			}
			return tuple, nil
		},
		DBPath:      func() (string, error) { return "db", nil },
		Poll:        time.Millisecond,
		OperationID: func() string { return "cancel-db" },
		ReadScreen: func(context.Context, string) (domain.Screen, error) {
			switch screenReads.Add(1) {
			case 1:
				return openCodeScreenFixture(openCodeNormalComposerFixture), nil
			case 2:
				return openCodeScreenFixture(openCodeShellComposerFixture), nil
			default:
				return openCodeScreenFixture(openCodeShellDraftFixture("pwd # herdr-tg-op:cancel-db")), nil
			}
		},
		SendKeys: func(context.Context, string, []string) error { return nil },
		SendText: func(context.Context, string, string) error { return nil },
		SQLite: func(_ context.Context, _, query string) ([]byte, error) {
			if strings.Contains(query, "SELECT id FROM") || strings.Contains(query, "LIKE") {
				return []byte(`[]`), nil
			}
			select {
			case <-databaseReadFailed:
			default:
				close(databaseReadFailed)
			}
			return nil, errors.New("private database path and credentials")
		},
	}
	done := make(chan error, 1)
	go func() {
		_, err := shell.Run(ctx, domain.Agent{Key: domain.Key{PaneID: "p", SessionDigest: tuple.Digest()}, Kind: domain.OpenCodeKind}, "pwd", nil, nil)
		done <- err
	}()
	select {
	case <-databaseReadFailed:
	case <-time.After(time.Second):
		t.Fatal("record database read did not fail")
	}
	select {
	case <-probeStarted:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("post-failure session lookup did not start")
	}
	select {
	case err := <-done:
		if stage, ok := domain.ShellErrorStage(err); !ok || stage != domain.ShellStageRecordRead {
			t.Fatalf("database failure mislabeled as %q (known=%v)", stage, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("shell watcher did not finish after cancellation")
	}
}
