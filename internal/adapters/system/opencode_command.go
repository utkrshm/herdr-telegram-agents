package system

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

const openCodeCommandMarker = "The following tool was executed by the user"
const openCodeToolRowMaxJSON = openCodeDBMaxOutput - (64 << 10)

// commandWithOperationID appends a shell comment so the stored tool input is
// unique to this submission without changing a supported single-line command.
// Syntax where the appended comment could be tokenized differently (including
// multiline, here-documents, backticks, and unquoted #) is refused.
func commandWithOperationID(command, operationID string) (string, error) {
	if strings.TrimSpace(command) == "" || operationID == "" || strings.ContainsAny(command, "\r\n") || strings.Contains(command, "<<") {
		return "", errors.New("command syntax cannot be safely correlated")
	}
	var quote byte
	escaped := false
	parenDepth := 0
	for index := 0; index < len(command); index++ {
		char := command[index]
		if escaped {
			escaped = false
			continue
		}
		if quote == '\'' {
			if char == '\'' {
				quote = 0
			}
			continue
		}
		if char == '\\' {
			escaped = true
			continue
		}
		if quote == 0 && char == '`' {
			return "", errors.New("backtick substitution cannot be safely correlated")
		}
		if quote == '"' {
			if char == '"' {
				quote = 0
			}
			continue
		}
		if char == '\'' || char == '"' {
			quote = char
			continue
		}
		if char == '(' {
			parenDepth++
		}
		if char == ')' {
			parenDepth--
			if parenDepth < 0 {
				return "", errors.New("command has unbalanced shell syntax")
			}
		}
		if char == '#' {
			return "", errors.New("unquoted comment syntax cannot be safely correlated")
		}
	}
	if quote != 0 || escaped || parenDepth != 0 {
		return "", errors.New("command has incomplete quoting, escaping, or shell syntax")
	}
	return command + " # herdr-tg-op:" + operationID, nil
}

type OpenCodeCommandResult struct {
	Output        string
	OutputPresent bool
	ExitStatus    *int
	Complete      bool
	Description   string
}

type OpenCodeCommandSnapshot map[string]struct{}

type openCodeCommandBaseline struct {
	ids OpenCodeCommandSnapshot
}

// SnapshotOpenCodeCommandParts takes a bounded baseline from the newest 2,000
// parts. Correlation also requires the synthetic user marker and exact input.
func SnapshotOpenCodeCommandParts(ctx context.Context, sqlite func(context.Context, string, string) ([]byte, error), dbPath, sessionID string) (OpenCodeCommandSnapshot, error) {
	if sqlite == nil || !openCodeSessionIDPattern.MatchString(sessionID) {
		return nil, errors.New("invalid OpenCode database reader or session")
	}
	query := fmt.Sprintf(`SELECT id FROM (SELECT id FROM part WHERE session_id='%s' ORDER BY time_created DESC LIMIT 2000)`, sessionID)
	raw, err := sqlite(ctx, dbPath, query)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(raw, &rows); err != nil {
		return nil, errors.New("invalid OpenCode part snapshot")
	}
	ids := make(OpenCodeCommandSnapshot, len(rows))
	for _, row := range rows {
		if row.ID == "" {
			return nil, errors.New("OpenCode part without id")
		}
		ids[row.ID] = struct{}{}
	}
	return ids, nil
}

func snapshotOpenCodeCommand(ctx context.Context, sqlite func(context.Context, string, string) ([]byte, error), dbPath, sessionID string) (openCodeCommandBaseline, error) {
	ids, err := SnapshotOpenCodeCommandParts(ctx, sqlite, dbPath, sessionID)
	if err != nil {
		return openCodeCommandBaseline{}, err
	}
	userQuery := fmt.Sprintf(`SELECT DISTINCT message_id AS id FROM (SELECT message_id,data FROM part WHERE session_id='%s' ORDER BY time_created DESC LIMIT 2000) WHERE json_extract(data,'$.type')='text' AND json_extract(data,'$.synthetic')=1 AND json_extract(data,'$.text') LIKE '%%%s%%'`, sessionID, openCodeCommandMarker)
	userRaw, err := sqlite(ctx, dbPath, userQuery)
	if err != nil {
		return openCodeCommandBaseline{}, err
	}
	var users []struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(userRaw, &users); err != nil {
		return openCodeCommandBaseline{}, errors.New("invalid OpenCode parent baseline")
	}
	for _, user := range users {
		if user.ID == "" {
			return openCodeCommandBaseline{}, errors.New("OpenCode synthetic parent without id")
		}
		ids["user:"+user.ID] = struct{}{}
	}
	return openCodeCommandBaseline{ids: ids}, nil
}

type OpenCodeCommandMessage struct {
	Role        string
	ParentID    string
	ParentText  string
	Synthetic   bool
	ParentKnown bool
	Parts       []OpenCodeCommandPart
}
type OpenCodeCommandPart struct {
	ID, Type, Tool, Command, Status, Output string
	OutputPresent                           bool
}

func CorrelateOpenCodeCommand(baseline OpenCodeCommandSnapshot, messages []OpenCodeCommandMessage, command string) (OpenCodeCommandResult, bool, error) {
	var matches []OpenCodeCommandPart
	newParents := map[string]struct{}{}
	untagged := false
	missingCommand := false
	for _, message := range messages {
		if message.ParentID == "" {
			return OpenCodeCommandResult{}, false, errors.New("OpenCode command parent ID is unavailable")
		}
		if _, existed := baseline["user:"+message.ParentID]; existed {
			continue
		}
		if !message.ParentKnown || !message.Synthetic || !strings.Contains(message.ParentText, openCodeCommandMarker) {
			return OpenCodeCommandResult{}, false, errors.New("new OpenCode command parent was not a verified synthetic shell request")
		}
		newParents[message.ParentID] = struct{}{}
		if message.Role != "assistant" {
			continue
		}
		for _, part := range message.Parts {
			if _, exists := baseline[part.ID]; exists {
				continue
			}
			if part.ID == "" || part.Type != "tool" || part.Tool != "bash" {
				continue
			}
			if part.Command == "" {
				missingCommand = true
				continue
			}
			if part.Command == command {
				matches = append(matches, part)
			} else {
				untagged = true
			}
		}
	}
	if missingCommand {
		return OpenCodeCommandResult{}, false, errors.New("OpenCode shell record has no verifiable command input")
	}
	if untagged {
		return OpenCodeCommandResult{}, false, errors.New("OpenCode did not preserve the request marker; output cannot be attributed safely")
	}
	if len(newParents) > 1 {
		return OpenCodeCommandResult{}, false, errors.New("ambiguous OpenCode synthetic command parents")
	}
	if len(matches) == 0 {
		return OpenCodeCommandResult{}, false, nil
	}
	if len(matches) != 1 {
		return OpenCodeCommandResult{}, false, errors.New("ambiguous OpenCode command record")
	}
	part := matches[0]
	if part.Status == "running" {
		return OpenCodeCommandResult{}, false, nil
	}
	if part.Status != "completed" {
		return OpenCodeCommandResult{}, false, fmt.Errorf("OpenCode command has terminal state %q", part.Status)
	}
	return OpenCodeCommandResult{Output: part.Output, OutputPresent: part.OutputPresent, Complete: true, Description: "OpenCode recorded completion; exit status is unavailable"}, true, nil
}

// OpenCodeShell activates the TUI's shell mode using a literal key event.
// It never sends the command through agent.prompt or bracketed paste.
type OpenCodeShell struct {
	Session            func(context.Context, string) (domain.SessionTuple, error)
	SQLite             func(context.Context, string, string) ([]byte, error)
	DBPath             func() (string, error)
	ReadScreen         func(context.Context, string) (domain.Screen, error)
	SendKeys           func(context.Context, string, []string) error
	SendText           func(context.Context, string, string) error
	Poll               time.Duration
	OperationID        func() string
	TransitionWait     time.Duration
	TransitionPoll     time.Duration
	ProbeTimeout       time.Duration
	ScreenProbeTimeout time.Duration
	ProgressAfter      time.Duration
}

var openCodeShellLocks sync.Map

func NewOpenCodeShell(session func(context.Context, string) (domain.SessionTuple, error), readScreen func(context.Context, string) (domain.Screen, error), sendKeys func(context.Context, string, []string) error, sendText func(context.Context, string, string) error) *OpenCodeShell {
	return &OpenCodeShell{Session: session, ReadScreen: readScreen, SendKeys: sendKeys, SendText: sendText, SQLite: runOpenCodeSQLite, DBPath: defaultOpenCodeDBPath, Poll: 5 * time.Second, OperationID: rand.Text, TransitionWait: 500 * time.Millisecond, TransitionPoll: 20 * time.Millisecond, ProbeTimeout: 5 * time.Second, ScreenProbeTimeout: 150 * time.Millisecond, ProgressAfter: 5 * time.Second}
}

func (s *OpenCodeShell) Run(ctx context.Context, agent domain.Agent, command string, submitted func() error, progress func(domain.Screen) error) (string, error) {
	if agent.Kind != domain.OpenCodeKind || agent.SessionDigest == "" || s.Session == nil || s.SQLite == nil || s.DBPath == nil || s.ReadScreen == nil || s.SendKeys == nil || s.SendText == nil {
		return "", errors.New("OpenCode shell unavailable")
	}
	idGenerator := s.OperationID
	if idGenerator == nil {
		idGenerator = rand.Text
	}
	toolCommand, err := commandWithOperationID(command, idGenerator())
	if err != nil {
		return "", err
	}
	session, err := s.exactSession(ctx, agent)
	if err != nil {
		return "", err
	}
	lockValue, _ := openCodeShellLocks.LoadOrStore(session.Value, make(chan struct{}, 1))
	lock := lockValue.(chan struct{})
	select {
	case lock <- struct{}{}:
		defer func() { <-lock }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if _, err = s.exactSession(ctx, agent); err != nil {
		return "", err
	}
	dbPath, err := s.DBPath()
	if err != nil {
		return "", errors.New("OpenCode database unavailable")
	}
	baselineCtx, baselineCancel := context.WithTimeout(ctx, s.probeTimeout())
	baseline, err := snapshotOpenCodeCommand(baselineCtx, s.SQLite, dbPath, session.Value)
	baselineCancel()
	if err != nil {
		return "", errors.New("could not take OpenCode command baseline")
	}
	if _, err = s.exactSession(ctx, agent); err != nil {
		return "", err
	}
	screen, err := s.readScreen(ctx, agent.PaneID)
	if err != nil {
		slog.Warn("OpenCode shell stopped before key event", "stage", "initial-composer", "screen_read_ok", err == nil)
		return "", errors.New("OpenCode prompt was not verifiably empty; command not sent")
	}
	if !openCodeInputIsSafeScreen(screen, true) {
		if !openCodeInputIsSafeScreen(screen, false) {
			slog.Warn("OpenCode shell stopped before key event", "stage", "initial-composer", "normal_or_shell_composer", false)
			return "", errors.New("OpenCode prompt was not verifiably empty; command not sent")
		}
		if _, err = s.exactSession(ctx, agent); err != nil {
			return "", err
		}
		if err = s.SendKeys(ctx, agent.PaneID, []string{"!"}); err != nil {
			slog.Warn("OpenCode shell stopped", "stage", "shell-mode-key", "key_event_ok", false)
			return "", errors.New("shell-mode key event failed; command not typed")
		}
		if err = s.waitForComposer(ctx, agent.PaneID, func(screen domain.Screen) bool { return openCodeInputIsSafeScreen(screen, true) }); err != nil {
			slog.Warn("OpenCode shell stopped", "stage", "shell-mode-verification", "key_event_ok", true, "shell_composer", false)
			return "", errors.New("OpenCode shell mode could not be verified; command not typed")
		}
	}
	if _, err = s.exactSession(ctx, agent); err != nil {
		return "", err
	}
	if err = s.SendText(ctx, agent.PaneID, toolCommand); err != nil {
		slog.Warn("OpenCode shell stopped", "stage", "typed-input", "send_text_ok", false)
		return "", errors.New("shell input failed; command submission is uncertain")
	}
	if err = s.waitForComposer(ctx, agent.PaneID, func(screen domain.Screen) bool { return openCodeShellDraftMatchesANSI(screen.ANSI, toolCommand) }); err != nil {
		slog.Warn("OpenCode shell stopped", "stage", "typed-input-verification", "send_text_ok", true, "draft_match", false)
		return "", errors.New("typed shell command could not be verified in OpenCode; Enter was not sent")
	}
	if _, err = s.exactSession(ctx, agent); err != nil {
		return "", err
	}
	preEnterAt := time.Now().UnixMilli()
	if err = s.SendKeys(ctx, agent.PaneID, []string{"enter"}); err != nil {
		return "", errors.New("shell submission is uncertain; command will not be resent")
	}
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	var completed atomic.Bool
	s.startProgressSnapshot(watchCtx, &completed, agent.PaneID, progress)
	if submitted != nil {
		_ = submitted()
	}
	interval := s.Poll
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-watchCtx.Done():
			return "", watchCtx.Err()
		case <-ticker.C:
		}
		if _, err = s.exactSession(watchCtx, agent); err != nil {
			return "", err
		}
		probeCtx, cancelProbe := context.WithTimeout(watchCtx, s.probeTimeout())
		messages, readErr := readOpenCodeCommandMessages(probeCtx, s.SQLite, dbPath, session.Value, preEnterAt)
		cancelProbe()
		if readErr != nil {
			if errors.Is(readErr, errOpenCodeDBOutputLimit) {
				return "", domain.ErrShellOutputTooLarge
			}
			continue
		}
		result, complete, matchErr := CorrelateOpenCodeCommand(baseline.ids, messages, toolCommand)
		if matchErr != nil {
			return "", matchErr
		}
		if complete {
			if _, err = s.exactSession(watchCtx, agent); err != nil {
				return "", err
			}
			if !result.OutputPresent {
				return "", errors.New("OpenCode command output was not verifiable")
			}
			completed.Store(true)
			cancelWatch()
			return result.Output, nil
		}
	}
}

func (s *OpenCodeShell) waitForComposer(ctx context.Context, pane string, verified func(domain.Screen) bool) error {
	wait := s.TransitionWait
	if wait <= 0 {
		wait = 500 * time.Millisecond
	}
	poll := s.TransitionPoll
	if poll <= 0 {
		poll = 20 * time.Millisecond
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		screen, err := s.readScreen(ctx, pane)
		if err == nil && verified(screen) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("composer state not verified")
		case <-ticker.C:
		}
	}
}

func (s *OpenCodeShell) startProgressSnapshot(ctx context.Context, completed *atomic.Bool, pane string, progress func(domain.Screen) error) {
	if progress == nil {
		return
	}
	delay := s.ProgressAfter
	if delay <= 0 {
		delay = 5 * time.Second
	}
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if completed.Load() {
			return
		}
		screenCtx, cancel := context.WithTimeout(ctx, s.screenProbeTimeout())
		snapshot, err := s.ReadScreen(screenCtx, pane)
		cancel()
		if err != nil {
			snapshot.Text = "OpenCode shell command has no verified result yet; screen snapshot unavailable."
		} else {
			snapshot.Text = "OpenCode shell command has no verified result yet.\n" + snapshot.Text
		}
		if completed.Load() || ctx.Err() != nil {
			return
		}
		_ = progress(snapshot)
	}()
}

func (s *OpenCodeShell) readScreen(ctx context.Context, pane string) (domain.Screen, error) {
	probeCtx, cancel := context.WithTimeout(ctx, s.screenProbeTimeout())
	defer cancel()
	return s.ReadScreen(probeCtx, pane)
}

func (s *OpenCodeShell) probeTimeout() time.Duration {
	if s.ProbeTimeout > 0 {
		return s.ProbeTimeout
	}
	return 5 * time.Second
}
func (s *OpenCodeShell) screenProbeTimeout() time.Duration {
	if s.ScreenProbeTimeout > 0 {
		return s.ScreenProbeTimeout
	}
	return 150 * time.Millisecond
}

func (s *OpenCodeShell) exactSession(ctx context.Context, agent domain.Agent) (domain.SessionTuple, error) {
	probeCtx, cancel := context.WithTimeout(ctx, s.probeTimeout())
	defer cancel()
	tuple, err := s.Session(probeCtx, agent.PaneID)
	if err != nil {
		return domain.SessionTuple{}, errors.New("Herdr OpenCode session lookup failed")
	}
	if tuple.Agent != domain.OpenCodeKind || tuple.Kind != "id" || tuple.Value == "" || tuple.Digest() != agent.SessionDigest {
		return domain.SessionTuple{}, errors.New("OpenCode session changed or could not be verified")
	}
	return tuple, nil
}

func readOpenCodeCommandMessages(ctx context.Context, sqlite func(context.Context, string, string) ([]byte, error), dbPath, sessionID string, preEnterAt int64) ([]OpenCodeCommandMessage, error) {
	if !openCodeSessionIDPattern.MatchString(sessionID) {
		return nil, errors.New("invalid OpenCode session")
	}
	query := fmt.Sprintf(`SELECT m.id AS message_id,m.data AS message_data,parent.data AS parent_data,parent_part.data AS parent_part_data,p.id AS part_id,CASE WHEN length(json_quote(p.data))>%d THEN NULL ELSE p.data END AS part_data,CASE WHEN length(json_quote(p.data))>%d THEN 1 ELSE 0 END AS part_too_large FROM message m LEFT JOIN message parent ON parent.id=json_extract(m.data,'$.parentID') LEFT JOIN part parent_part ON parent_part.message_id=parent.id AND CAST(parent_part.time_created AS INTEGER)>=%d AND json_extract(parent_part.data,'$.type')='text' LEFT JOIN part p ON p.message_id=m.id WHERE m.session_id='%s' AND parent_part.message_id IS NOT NULL ORDER BY parent_part.time_created,p.time_created LIMIT 501`, openCodeToolRowMaxJSON, openCodeToolRowMaxJSON, preEnterAt, sessionID)
	raw, err := sqlite(ctx, dbPath, query)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		MessageID      string `json:"message_id"`
		MessageData    string `json:"message_data"`
		ParentData     string `json:"parent_data"`
		ParentPartData string `json:"parent_part_data"`
		PartID         string `json:"part_id"`
		PartData       string `json:"part_data"`
		PartTooLarge   int    `json:"part_too_large"`
	}
	if err = json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	if len(rows) > 500 {
		return nil, errors.New("OpenCode result window exceeded safe limit")
	}
	byID := map[string]*OpenCodeCommandMessage{}
	order := []string{}
	for _, row := range rows {
		if row.PartTooLarge != 0 {
			return nil, errOpenCodeDBOutputLimit
		}
		msg := byID[row.MessageID]
		if msg == nil {
			var data struct {
				Role     string `json:"role"`
				ParentID string `json:"parentID"`
			}
			if json.Unmarshal([]byte(row.MessageData), &data) != nil {
				return nil, errors.New("invalid OpenCode message record")
			}
			parentUser, parentKnown := openCodeParent(row.ParentData)
			if row.ParentData == "" {
				return nil, errors.New("missing OpenCode parent message")
			}
			parentText, syntheticPart, textPartFound := openCodeTextPart(row.ParentPartData)
			if !textPartFound {
				return nil, errors.New("OpenCode synthetic text part is unavailable")
			}
			msg = &OpenCodeCommandMessage{Role: data.Role, ParentID: data.ParentID, ParentText: parentText, Synthetic: parentUser && syntheticPart, ParentKnown: parentKnown && parentUser && textPartFound}
			byID[row.MessageID] = msg
			order = append(order, row.MessageID)
		}
		if row.PartID == "" || row.PartData == "" {
			continue
		}
		var part struct {
			Type    string  `json:"type"`
			Tool    string  `json:"tool"`
			Command string  `json:"command"`
			Output  *string `json:"output"`
			Status  string  `json:"status"`
			State   struct {
				Status string `json:"status"`
				Input  struct {
					Command string `json:"command"`
				} `json:"input"`
				Output *string `json:"output"`
			} `json:"state"`
			Text string `json:"text"`
		}
		if json.Unmarshal([]byte(row.PartData), &part) != nil {
			return nil, errors.New("invalid OpenCode part record")
		}
		status := part.Status
		if status == "" {
			status = part.State.Status
		}
		command := part.Command
		if command == "" {
			command = part.State.Input.Command
		}
		var output string
		present := false
		if part.Output != nil {
			output = *part.Output
			present = true
		} else if part.State.Output != nil {
			output = *part.State.Output
			present = true
		}
		msg.Parts = append(msg.Parts, OpenCodeCommandPart{ID: row.PartID, Type: part.Type, Tool: part.Tool, Command: command, Status: status, Output: output, OutputPresent: present})
		if part.Tool != "" {
			msg.Role = "assistant"
		}
		if part.Type == "text" && msg.ParentText == "" {
			msg.ParentText = part.Text
		}
	}
	out := make([]OpenCodeCommandMessage, 0, len(order))
	for _, id := range order {
		out = append(out, *byID[id])
	}
	return out, nil
}

func openCodeParent(data string) (bool, bool) {
	var parent struct {
		Role string `json:"role"`
	}
	if json.Unmarshal([]byte(data), &parent) != nil {
		return false, false
	}
	return parent.Role == "user", true
}

func openCodeTextPart(data string) (string, bool, bool) {
	var part struct {
		Type      string `json:"type"`
		Text      string `json:"text"`
		Synthetic bool   `json:"synthetic"`
	}
	if json.Unmarshal([]byte(data), &part) == nil && part.Type == "text" {
		return part.Text, part.Synthetic, true
	}
	return "", false, false
}

func openCodeEmptyInput(screen string) bool { return openCodeComposerMode(screen) == "normal" }
func openCodeShellInput(screen string) bool { return openCodeComposerMode(screen) == "shell" }
func openCodeInputIsSafe(screen string, shell bool) bool {
	if openCodeHasModal(screen) {
		return false
	}
	if shell {
		return openCodeShellInput(screen)
	}
	return openCodeEmptyInput(screen)
}

func openCodeInputIsSafeScreen(screen domain.Screen, shell bool) bool {
	// Plain text cannot tell transcript from composer. Fail closed if the
	// ANSI screen is unavailable rather than guessing from transcript geometry.
	if screen.ANSI == "" || openCodeHasModal(screen.Text) {
		return false
	}
	want := map[bool]string{true: "shell", false: "normal"}[shell]
	return openCodeANSIComposerMode(screen.ANSI) == want && openCodeANSIComposerEmpty(screen.ANSI, want)
}

func openCodeShellDraftMatches(screen, command string) bool {
	if openCodeHasModal(screen) {
		return false
	}
	lines := strings.Split(screen, "\n")
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) < 4 {
		return false
	}
	border := -1
	for index := max(0, len(lines)-8); index < len(lines); index++ {
		if strings.Contains(lines[index], "╹") {
			border = index
			break
		}
	}
	if border < 2 || border+1 >= len(lines) || strings.TrimSpace(lines[border+1]) == "" {
		return false
	}
	metadataIndex := border - 1
	metadataRail := strings.IndexRune(lines[metadataIndex], '┃')
	if metadataRail < 0 || !openCodeShellMetadata(lines[metadataIndex][metadataRail+len("┃"):]) {
		return false
	}
	var draftRows []string
	for index := metadataIndex - 1; index >= 0; index-- {
		line := lines[index]
		rail := strings.IndexRune(line, '┃')
		if rail < 0 {
			break
		}
		text := strings.TrimSpace(line[rail+len("┃"):])
		draftRows = append([]string{text}, draftRows...)
	}
	return len(draftRows) == 1 && draftRows[0] == command
}

func openCodeShellDraftMatchesANSI(screen, command string) bool {
	if screen == "" {
		return false
	}
	plain := stripOpenCodeANSI(screen)
	if openCodeHasModal(plain) || openCodeANSIComposerMode(screen) != "shell" {
		return false
	}
	rows := openCodeStyledRows(screen)
	for border := max(0, len(rows)-8); border < len(rows); border++ {
		if !strings.Contains(rows[border].text, "╹") || border < 2 || border+1 >= len(rows) || strings.TrimSpace(rows[border+1].text) == "" {
			continue
		}
		metadata := rows[border-1]
		rail := strings.IndexRune(metadata.text, '┃')
		if rail < 0 || !metadata.hasBackground(30, 30, 30) || !openCodeShellMetadata(strings.TrimSpace(metadata.text[rail+len("┃"):])) {
			continue
		}
		var draft string
		for row := border - 2; row >= 0 && rows[row].hasBackground(30, 30, 30); row-- {
			line := strings.TrimRight(rows[row].text, " ")
			rail := strings.IndexRune(line, '┃')
			if rail < 0 {
				return false
			}
			content := strings.TrimSpace(line[rail+len("┃"):])
			if content == "" {
				continue
			}
			if draft != "" {
				return false
			}
			draft = content
		}
		return draft == command
	}
	return false
}

// OpenCode 1.19.6 draws its active composer on the #1e1e1e surface; transcript
// rows use #141414 and the surrounding shell uses #0a0a0a. Require this color
// on the exact metadata row, plus the expected rail/bottom-border geometry.
// This intentionally rejects themes or renderings we have not captured.
func openCodeANSIComposerMode(screen string) string {
	rows := openCodeStyledRows(screen)
	for border := max(0, len(rows)-8); border < len(rows); border++ {
		if !strings.Contains(rows[border].text, "╹") || border < 2 || border+1 >= len(rows) || strings.TrimSpace(rows[border+1].text) == "" {
			continue
		}
		meta := rows[border-1]
		rail := strings.IndexRune(meta.text, '┃')
		if rail < 0 || !meta.hasBackground(30, 30, 30) {
			continue
		}
		label := strings.TrimSpace(meta.text[rail+len("┃"):])
		if label == "" {
			continue
		}
		mode := "normal"
		if openCodeShellMetadata(label) {
			mode = "shell"
		}
		placeholder := "Ask anything…"
		if mode == "shell" {
			placeholder = "Run a command…"
		}
		for row := border - 2; row >= 0; row-- {
			if !rows[row].hasBackground(30, 30, 30) {
				break // transcript or surrounding UI, not part of the composer
			}
			line := strings.TrimRight(rows[row].text, " ")
			if strings.TrimSpace(line) == "" {
				continue
			}
			rail := strings.IndexRune(line, '┃')
			if rail < 0 {
				break
			}
			content := strings.TrimSpace(line[rail+len("┃"):])
			if content != "" && content != placeholder {
				return mode // mode only; draft verification is a separate exact match
			}
		}
		return mode
	}
	return ""
}

func openCodeANSIComposerEmpty(screen, mode string) bool {
	rows := openCodeStyledRows(screen)
	for border := max(0, len(rows)-8); border < len(rows); border++ {
		if !strings.Contains(rows[border].text, "╹") || border < 2 {
			continue
		}
		placeholder := "Ask anything…"
		if mode == "shell" {
			placeholder = "Run a command…"
		}
		inputRows := 0
		for row := border - 2; row >= 0 && rows[row].hasBackground(30, 30, 30); row-- {
			inputRows++
			line := strings.TrimRight(rows[row].text, " ")
			rail := strings.IndexRune(line, '┃')
			if rail < 0 {
				return false
			}
			content := strings.TrimSpace(line[rail+len("┃"):])
			if content != "" && content != placeholder {
				return false
			}
		}
		// The metadata and bottom border alone do not prove that an input
		// region was observed. Require a positively styled, rail-bearing row.
		return inputRows > 0
	}
	return false
}

type openCodeStyledRow struct {
	text string
	bgs  map[[3]int]bool
}

func (r openCodeStyledRow) hasBackground(red, green, blue int) bool {
	return r.bgs[[3]int{red, green, blue}]
}

func openCodeStyledRows(input string) []openCodeStyledRow {
	rows := []openCodeStyledRow{{bgs: make(map[[3]int]bool)}}
	bg := [3]int{}
	for index := 0; index < len(input); {
		if input[index] == '\x1b' && index+1 < len(input) && input[index+1] == '[' {
			start := index + 2
			index = start
			for index < len(input) && (input[index] < 0x40 || input[index] > 0x7e) {
				index++
			}
			if index < len(input) && input[index] == 'm' {
				bg = openCodeANSIBackground(input[start:index], bg)
			}
			if index < len(input) {
				index++
			}
			continue
		}
		if input[index] == '\n' {
			rows = append(rows, openCodeStyledRow{bgs: make(map[[3]int]bool)})
			index++
			continue
		}
		runeValue, size := utf8.DecodeRuneInString(input[index:])
		row := &rows[len(rows)-1]
		row.text += string(runeValue)
		if bg != [3]int{} {
			row.bgs[bg] = true
		}
		index += size
	}
	return rows
}

func openCodeANSIBackground(parameters string, current [3]int) [3]int {
	fields := strings.Split(parameters, ";")
	for i := 0; i < len(fields); i++ {
		if fields[i] == "0" {
			current = [3]int{}
		}
		if fields[i] == "48" && i+4 < len(fields) && fields[i+1] == "2" {
			var rgb [3]int
			for channel := range rgb {
				rgb[channel], _ = strconv.Atoi(fields[i+2+channel])
			}
			return rgb
		}
	}
	return current
}

func stripOpenCodeANSI(value string) string {
	var out strings.Builder
	for index := 0; index < len(value); {
		if value[index] == '\x1b' && index+1 < len(value) && value[index+1] == '[' {
			index += 2
			for index < len(value) && (value[index] < 0x40 || value[index] > 0x7e) {
				index++
			}
			if index < len(value) {
				index++
			}
			continue
		}
		out.WriteByte(value[index])
		index++
	}
	return out.String()
}

func openCodeHasModal(screen string) bool {
	if len(domain.ParseDialog(screen).Choices) > 0 {
		return true
	}
	for _, line := range strings.Split(strings.ToLower(screen), "\n") {
		if strings.Contains(line, "enter to select") && strings.Contains(line, "esc to cancel") {
			return true
		}
	}
	return false
}

// The mode label is left-aligned; cwd and branch metadata may follow it on
// the same row, aligned to the right edge of the terminal.
func openCodeShellMetadata(value string) bool {
	fields := strings.Fields(value)
	return len(fields) >= 1 && fields[0] == "Shell"
}

// OpenCode 1.19.6 renders blank composer rows, a metadata row, the bottom
// border, and then the footer. The left rail identifies the composer itself;
// only blank rows (or OpenCode's known empty-input placeholder) are accepted.
func openCodeComposerMode(screen string) string {
	lines := strings.Split(screen, "\n")
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) == 0 {
		return ""
	}
	border := -1
	for index := max(0, len(lines)-8); index < len(lines); index++ {
		if strings.Contains(lines[index], "╹") {
			border = index
			break
		}
	}
	if border < 2 || border+1 >= len(lines) || strings.TrimSpace(lines[border+1]) == "" {
		return ""
	}
	metadataIndex := border - 1
	metadataLine := lines[metadataIndex]
	metadataRail := strings.IndexRune(metadataLine, '┃')
	if metadataRail < 0 {
		return ""
	}
	metadata := strings.TrimSpace(metadataLine[metadataRail+len("┃"):])
	mode := "normal"
	if openCodeShellMetadata(metadata) {
		mode = "shell"
	}
	if metadata == "" {
		return ""
	}
	placeholder := "Ask anything…"
	if mode == "shell" {
		placeholder = "Run a command…"
	}
	emptyRows := 0
	for index := metadataIndex - 1; index >= 0; index-- {
		line := lines[index]
		rail := strings.IndexRune(line, '┃')
		if rail < 0 {
			if strings.TrimSpace(line) != "" {
				return ""
			}
			break
		}
		content := strings.TrimSpace(line[rail+len("┃"):])
		if content != "" {
			if content == placeholder {
				emptyRows++
				continue
			}
			// ReadScreen strips ANSI, so transcript and composer text share the
			// same rail and plain-text geometry cannot prove this row is outside
			// the input. Never infer a blank composer from adjacent blank rows.
			return ""
		}
		emptyRows++
	}
	if emptyRows == 0 {
		return ""
	}
	return mode
}
