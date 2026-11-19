package app

import (
	"context"
	"errors"
	"strings"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

type shellResult struct {
	key        domain.Key
	threadID   int
	messageID  int
	generation *topicGeneration
	output     string
	err        error
	submitted  bool
}

type shellAck struct {
	key                 domain.Key
	threadID, messageID int
	generation          *topicGeneration
}

type shellProgress struct {
	key                 domain.Key
	threadID, messageID int
	generation          *topicGeneration
	screen              string
}

type shellStatusResult struct {
	key                 domain.Key
	threadID, messageID int
	generation          *topicGeneration
	screen              string
	err                 error
}

// shellStatusCommand reads the exact mapped OpenCode pane in the background;
// it never sends keys, text, or a command to Herdr.
func (i *inbound) shellStatusCommand(ctx context.Context, msg domain.TopicMessage, key domain.Key, agent domain.Agent) error {
	if agent.Kind != domain.OpenCodeKind {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "/shell-status is available only for OpenCode agents.")
	}
	generation, err := i.topics.generation(key, msg.ThreadID)
	if err != nil {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "OpenCode topic is no longer available.")
	}
	i.asyncLong(func(workerCtx context.Context) any {
		result := shellStatusResult{key: key, threadID: msg.ThreadID, messageID: msg.MessageID, generation: generation}
		call, release, guardErr := generation.guard(workerCtx)
		if guardErr != nil {
			result.err = guardErr
			return result
		}
		defer release()
		current, live := i.agents(key)
		if !live || current.Key != key || current.Kind != domain.OpenCodeKind {
			result.err = errors.New("OpenCode agent is no longer available")
			return result
		}
		screen, readErr := i.herdr.ReadScreen(call, key.PaneID, domain.ScreenVisible, 80)
		if readErr != nil {
			result.err = readErr
			return result
		}
		result.screen = screen.Text
		return result
	})
	return nil
}

func (i *inbound) shellStatusFinished(ctx context.Context, result shellStatusResult) error {
	if result.err != nil {
		return i.replyGuarded(ctx, result.threadID, result.messageID, "OpenCode visible screen snapshot is unavailable; no command was run.", result.generation.guard)
	}
	text, _ := cleanScreen(result.screen, i.chrome())
	if strings.TrimSpace(text) == "" {
		text = "(screen is empty)"
	}
	return i.replyGuarded(ctx, result.threadID, result.messageID, "OpenCode visible screen snapshot (screen content does not establish whether a shell command is running):\n"+text, result.generation.guard)
}

func (i *inbound) shellCommand(ctx context.Context, msg domain.TopicMessage, key domain.Key, agent domain.Agent, command string) error {
	if agent.Kind != domain.OpenCodeKind {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "/shell is supported only for OpenCode.")
	}
	if !agent.Status.ReadyForInput() {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "/shell requires an idle agent with no open dialog.")
	}
	if i.shell == nil {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "OpenCode shell commands are unavailable.")
	}
	if i.shellPending[key] {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "An OpenCode shell command is already being watched for this agent.")
	}
	if len(i.shellPending) >= 8 {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "Too many OpenCode shell commands are being watched; retry later.")
	}
	screen, err := i.herdr.ReadScreen(ctx, key.PaneID, domain.ScreenVisible, 80)
	if err != nil {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "Could not verify the OpenCode prompt; command not sent.")
	}
	if len(domain.ParseDialog(screen.Text).Choices) > 0 {
		return i.reply(ctx, msg.ThreadID, msg.MessageID, "/shell requires an idle agent with no open dialog.")
	}
	generation, err := i.topics.generation(key, msg.ThreadID)
	if err != nil {
		return nil
	}
	i.shellPending[key] = true
	i.asyncLong(func(workerCtx context.Context) any {
		// A queued shell request must not land in a replacement topic/session.
		if workerCtx.Err() != nil || !i.shellTargetCurrent(key, msg.ThreadID, generation) {
			return shellResult{key: key, threadID: msg.ThreadID, messageID: msg.MessageID, generation: generation, err: errors.New("topic or session changed before submission")}
		}
		guardedCtx, release, guardErr := generation.guard(workerCtx)
		if guardErr != nil {
			return shellResult{key: key, threadID: msg.ThreadID, messageID: msg.MessageID, generation: generation, err: errors.New("topic changed before shell submission")}
		}
		defer release()
		current, live := i.agents(key)
		if !live || current.Key != key || current.Kind != domain.OpenCodeKind || !current.Status.ReadyForInput() {
			return shellResult{key: key, threadID: msg.ThreadID, messageID: msg.MessageID, generation: generation, err: errors.New("OpenCode agent is no longer idle; command not sent")}
		}
		latest, err := i.herdr.ReadScreen(guardedCtx, key.PaneID, domain.ScreenVisible, 80)
		if err != nil || len(domain.ParseDialog(latest.Text).Choices) > 0 {
			return shellResult{key: key, threadID: msg.ThreadID, messageID: msg.MessageID, generation: generation, err: errors.New("could not verify an idle OpenCode prompt; command not sent")}
		}
		wasSubmitted := false
		submitted := func() error {
			wasSubmitted = true
			ack := shellAck{key: key, threadID: msg.ThreadID, messageID: msg.MessageID, generation: generation}
			if i.submitShellAck != nil {
				return i.submitShellAck(ack)
			}
			return i.shellAckFinished(guardedCtx, ack)
		}
		progress := func(screen domain.Screen) error {
			job := shellProgress{key: key, threadID: msg.ThreadID, messageID: msg.MessageID, generation: generation, screen: screen.Text}
			if i.submitShellProgress != nil {
				return i.submitShellProgress(job)
			}
			return i.shellProgressFinished(guardedCtx, job)
		}
		output, err := i.shell(guardedCtx, current, command, submitted, progress)
		return shellResult{key: key, threadID: msg.ThreadID, messageID: msg.MessageID, generation: generation, output: output, err: err, submitted: wasSubmitted}
	})
	return nil
}

func (i *inbound) shellTargetCurrent(key domain.Key, threadID int, generation *topicGeneration) bool {
	current, err := i.topics.generation(key, threadID)
	return err == nil && current == generation
}

func (i *inbound) shellFinished(ctx context.Context, result shellResult) error {
	delete(i.shellPending, result.key)
	if !i.shellTargetCurrent(result.key, result.threadID, result.generation) {
		return nil
	}
	if live, ok := i.agents(result.key); !ok || live.Key != result.key || live.Kind != domain.OpenCodeKind {
		return nil
	}
	if result.err != nil {
		if errors.Is(result.err, domain.ErrShellOutputTooLarge) {
			return i.reply(ctx, result.threadID, result.messageID, "OpenCode shell result exceeds the 8 MiB safe read limit. No partial output was delivered, and the command was not rerun.")
		}
		if result.submitted {
			return i.reply(ctx, result.threadID, result.messageID, "OpenCode accepted the command, but its recorded result is unavailable. It was not resent; check the agent before retrying.")
		}
		return i.reply(ctx, result.threadID, result.messageID, "OpenCode did not confirm command submission. No automatic retry was made; check the pane before retrying.")
	}
	text := result.output
	if strings.TrimSpace(text) == "" {
		text = "(no output)"
	}
	text += "\n\nOpenCode recorded completion; an exit status is not exposed by the session record."
	if len(text) > 3500 {
		err := i.tg.SendDocument(ctx, domain.Document{ThreadID: result.threadID, Name: "opencode-shell-output.txt", Data: []byte(text), Caption: "OpenCode shell output; recorded completion, exit status unavailable", ReplyTo: result.messageID})
		if err != nil {
			_ = i.reply(ctx, result.threadID, result.messageID, "OpenCode recorded a result, but Telegram could not deliver the output file. Check bot/topic access; the command was not rerun.")
		}
		return err
	}
	return i.reply(ctx, result.threadID, result.messageID, text)
}

func (i *inbound) shellAckFinished(ctx context.Context, ack shellAck) error {
	if !i.shellTargetCurrent(ack.key, ack.threadID, ack.generation) {
		return nil
	}
	return i.reply(ctx, ack.threadID, ack.messageID, "The command was entered in OpenCode shell mode and Enter was sent. Waiting for recorded output; it will not be resent.")
}

func (i *inbound) shellProgressFinished(ctx context.Context, progress shellProgress) error {
	if !i.shellTargetCurrent(progress.key, progress.threadID, progress.generation) {
		return nil
	}
	text := "OpenCode shell watcher progress snapshot:\n" + progress.screen
	if len(text) > 3500 {
		return i.tg.SendDocument(ctx, domain.Document{ThreadID: progress.threadID, Name: "opencode-shell-progress.txt", Data: []byte(text), Caption: "OpenCode shell progress snapshot", ReplyTo: progress.messageID})
	}
	return i.reply(ctx, progress.threadID, progress.messageID, text)
}
