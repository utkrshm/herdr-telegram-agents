package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/permgps/herdr-telegram-agents/internal/domain"
)

// Bridge is the goroutine that carries messages between Herdr and the
// topics: screens out on blocked and done, operator prompts, keys and
// commands in. It is fed by the daemon loop through Submit so the loop
// queues work with bounded backpressure while screen reads and commands
// share the serial Telegram queue. Fatal Telegram errors are reported
// through Fatal.
type Bridge struct {
	PrivateControl    *PrivateControl
	PrivateReconciler *PrivateReconciler
	Shares            *SharePanel
	PrivateHandler    func(context.Context, domain.PrivateMessage) error
	out               *outbound
	in                *inbound
	// jobs carries agent state. control preserves the order of
	// operator actions and asynchronous results under load.
	jobs               chan any
	control            chan any
	privateJobs        chan any
	ownerReplies       chan ownerReply
	ownerReplySlots    chan struct{}
	ownerNoticeSlot    chan struct{}
	promptNotices      chan promptNotice
	failedOwnerReplies []ownerReplyResult // bridge-owned, independently bounded quarantine
	PrivateBusy        func(int64)
	// fatal carries the first fatal error; the daemon reads it once.
	fatal   chan error
	dropped atomic.Int64
	// handled counts jobs taken off the queue; tests pace a burst by it.
	handled atomic.Int64
	log     *slog.Logger
	// runCtx is the context of Run; spawn derives its goroutines from it
	// and wg counts them so Run returns only when they are done.
	runCtx context.Context
	wg     sync.WaitGroup

	// CallTimeout bounds one job; tests shorten it.
	CallTimeout        time.Duration
	transferTimeout    time.Duration
	herdrServerTimeout time.Duration
	pendingUpdate      *domain.UpdateJob
}

// RestoreUpdate schedules a persisted progress view for the first bridge
// loop iteration, after the Telegram queue has started.
func (b *Bridge) RestoreUpdate(job domain.UpdateJob) {
	if job.PanelMessageID > 0 && !job.Terminal() {
		b.pendingUpdate = &job
	}
}

// Services are the optional machine-side helpers the bridge uses: the
// agent's transcript for done posts, git for /git and the inbox for
// attachments. A nil member disables that feature with a notice.
type Services struct {
	Replies          domain.ReplySource
	ExportTranscript func(context.Context, domain.Agent) ([]byte, error)
	Git              domain.GitRunner
	// Shell runs one already-validated OpenCode shell request through its
	// transcript store. It returns only after an exact result is recorded.
	Shell func(context.Context, domain.Agent, string, func() error, func(context.Context, domain.Screen) error) (string, error)
	Inbox domain.InboxStore
	// Config saves config.json when /observers changes the observer list;
	// nil refuses the change with a notice.
	Config        domain.ConfigStore
	Updates       *UpdateManager
	UpdateJobs    domain.UpdateJobStore
	LaunchUpdate  func(context.Context, string) (int, error)
	UpdateRunning func() bool
	// HerdrServer controls only the configured default Herdr server. The
	// callback is invoked off the bridge loop because stopping it disconnects
	// this daemon's Herdr socket.
	HerdrServer func(context.Context, bool) error
}

// NewBridge wires the outbound and inbound use cases around the registry,
// the reconciler's read model of the mapping and the screen capture.
// replies supplies the agent's last reply for the done post when the
// posts.done option asks for it; nil keeps every post screen-based.
func NewBridge(cfg domain.Config, herdr domain.HerdrGateway, tg domain.TelegramGateway,
	registry *Registry, reconciler *Reconciler, capture *Capture, opts *Options, svc Services, clock domain.Clock, log *slog.Logger) *Bridge {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if opts == nil {
		opts = NewOptions(domain.DefaultOptions(), nil, nil, log)
	}
	topics := reconciler.topics()
	// Every post of the bridge passes the redactor; the reconciler keeps
	// the raw gateway because topic names are agent labels.
	tg = newRedactingGateway(tg, domain.NewRedactor(cfg.BotToken), opts.RedactEnabled, log)
	out := newOutbound(herdr, tg, cfg.ChatID, cfg.OperatorIDs, topics, registry.Agent, registry.Live, capture, opts, svc.Replies, clock, log)
	in := newInbound(herdr, tg, topics, registry.Agent, registry.Live, out, opts, svc, cfg, clock, log)
	b := &Bridge{
		out:                out,
		in:                 in,
		jobs:               make(chan any, bridgeBuffer),
		control:            make(chan any, bridgeBuffer),
		privateJobs:        make(chan any, 32),
		ownerReplies:       make(chan ownerReply, ownerReplyCapacity),
		ownerReplySlots:    make(chan struct{}, ownerReplyCapacity),
		ownerNoticeSlot:    make(chan struct{}, 1),
		promptNotices:      make(chan promptNotice, 64),
		fatal:              make(chan error, 1),
		log:                log,
		CallTimeout:        bridgeCallTimeout,
		transferTimeout:    transferWorkerTimeout,
		herdrServerTimeout: asyncTimeout,
	}
	in.panel.chatID, in.panel.operators = cfg.ChatID, append([]int64(nil), cfg.OperatorIDs...)
	in.panel.updates, in.panel.jobs = svc.Updates, svc.UpdateJobs
	in.panel.launch, in.panel.running = svc.LaunchUpdate, svc.UpdateRunning
	in.panel.async = func(run func(context.Context) any) {
		b.spawn(func(ctx context.Context) {
			if err := b.SubmitContext(ctx, run(ctx)); err != nil && ctx.Err() == nil {
				b.log.Warn("update result not accepted", slog.String("err", err.Error()))
			}
		})
	}
	// Slow work (agent.start, a file download) runs off the loop and
	// reports back as a job, so the bridge stays the only writer to
	// Telegram.
	in.async = func(run func(context.Context) any) {
		b.spawn(func(ctx context.Context) {
			if err := b.SubmitContext(ctx, run(ctx)); err != nil && ctx.Err() == nil {
				b.log.Warn("[FIX] bridge async result not accepted", slog.String("err", err.Error()))
			}
		})
	}
	// A shell command may remain unrecorded after successful submission. Its
	// watcher is intentionally not subject to asyncTimeout and never resends.
	in.asyncLong = func(run func(context.Context) any) {
		parent := b.runCtx
		if parent == nil {
			parent = context.Background()
		}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			job := run(parent)
			if parent.Err() == nil {
				if err := b.SubmitContext(parent, job); err != nil && parent.Err() == nil {
					b.log.Warn("shell result not accepted", slog.String("err", err.Error()))
				}
			}
		}()
	}
	in.herdrServerAsync = func(run func(context.Context) herdrServerResult) {
		parent := b.runCtx
		if parent == nil {
			parent = context.Background()
		}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			timeout := b.herdrServerTimeout
			if timeout <= 0 {
				timeout = asyncTimeout
			}
			workCtx, cancel := context.WithTimeout(parent, timeout)
			result := run(workCtx)
			cancel()
			if parent.Err() == nil {
				if err := b.SubmitContext(parent, result); err != nil && parent.Err() == nil {
					b.log.Warn("Herdr server result not accepted", slog.String("err", err.Error()))
				}
			}
		}()
	}
	in.submitShellAck = func(ack shellAck) error {
		parent := b.runCtx
		if parent == nil {
			parent = context.Background()
		}
		return b.SubmitContext(parent, ack)
	}
	in.submitShellProgress = func(progress shellProgress) error {
		parent := b.runCtx
		if parent == nil {
			parent = context.Background()
		}
		return b.SubmitContext(parent, progress)
	}
	in.submitShellProgressContext = func(ctx context.Context, progress shellProgress) error {
		return b.SubmitContext(ctx, progress)
	}
	in.transferAsync = func(run func(context.Context) transferResult) {
		b.spawnTransfer(run)
	}
	in.promptNotice = func(_ context.Context, key domain.Key, threadID, messageID int, confirmed bool) error {
		gen, err := in.topics.generation(key, threadID)
		if err != nil {
			b.log.Warn("prompt notice could not pin topic generation", slog.String("key", key.String()), slog.Int("thread_id", threadID), slog.String("err", err.Error()))
			return nil
		}
		select {
		case b.promptNotices <- promptNotice{key: key, threadID: threadID, messageID: messageID, confirmed: confirmed, generation: gen}:
			return nil
		default:
			return fmt.Errorf("prompt notice queue full")
		}
	}
	return b
}

type promptNotice struct {
	key        domain.Key
	threadID   int
	messageID  int
	confirmed  bool
	generation *topicGeneration
}

// runPromptNotices serializes prompt acknowledgements and their screen
// snapshots. A slow capture cannot block the bridge loop, and FIFO processing
// keeps successive prompts in a topic in their original order.
func (b *Bridge) runPromptNotices(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case notice := <-b.promptNotices:
			callCtx, cancel := context.WithTimeout(ctx, asyncTimeout)
			err := b.in.postPromptNotice(callCtx, notice.key, notice.threadID, notice.messageID, notice.confirmed, notice.generation)
			cancel()
			if err == nil || errors.Is(err, errOwnerTopicChanged) || ctx.Err() != nil {
				continue
			}
			if isFatal(err) {
				select {
				case b.fatal <- err:
				default:
				}
				return
			}
			b.log.Warn("post-prompt notice failed", slog.String("key", notice.key.String()), slog.String("err", err.Error()))
		}
	}
}

// startResult is the job a background agent start submits when it is
// over: what was asked, what Herdr answered and when it began.
type startResult struct {
	messageID int
	workspace string
	kind      string
	name      string
	paneID    string
	agent     domain.Agent
	err       error
	started   time.Time
}

// spawn runs fn on its own goroutine with a context bound to Run and
// asyncTimeout; Run waits for every spawned goroutine.
func (b *Bridge) spawn(fn func(context.Context)) {
	parent := b.runCtx
	if parent == nil {
		parent = context.Background()
	}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		ctx, cancel := context.WithTimeout(parent, asyncTimeout)
		defer cancel()
		fn(ctx)
	}()
}

// spawnTransfer bounds the upload worker independently but reports its
// partial result using the bridge lifetime context, so worker timeout does
// not discard the outcome. Shutdown still cancels both work and delivery.
func (b *Bridge) spawnTransfer(run func(context.Context) transferResult) {
	parent := b.runCtx
	if parent == nil {
		parent = context.Background()
	}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		timeout := b.transferTimeout
		if timeout <= 0 {
			timeout = transferWorkerTimeout
		}
		worker, cancel := context.WithTimeout(parent, timeout)
		defer cancel()
		result := run(worker)
		if parent.Err() != nil {
			return
		}
		if err := b.SubmitContext(parent, result); err != nil && parent.Err() == nil {
			b.log.Warn("transfer result not accepted", slog.String("err", err.Error()))
		}
	}()
}

// presenceAway is the job the daemon submits when quiet mode ends: the
// outbound posts the questions that are still waiting.
type presenceAway struct{}

// SetPresence wires quiet mode into the outbound (posts mode, catch-up)
// and the inbound (/away, /here, the /status header).
func (b *Bridge) SetPresence(p *Presence, opts *Options) {
	if p == nil {
		return
	}
	b.out.SetPresence(p.Quiet, opts)
	b.in.SetPresence(p)
}

// SetPagerReachable tells the outbound whether the bot may write to at
// least one operator's private chat; the daemon calls it after its start
// probe and when the posts.pager option turns on. Safe from any goroutine.
func (b *Bridge) SetPagerReachable(ok bool) { b.out.SetPagerReachable(ok) }

// PagerReachable reports the flag last set by SetPagerReachable, or
// cleared by the outbound when every operator's chat refused a message.
func (b *Bridge) PagerReachable() bool { return b.out.PagerReachable() }

// SetStatusSince wires the dashboard's record of status start times into
// /status so it shows the same durations.
func (b *Bridge) SetStatusSince(fn func() map[domain.Key]time.Time) { b.in.SetSince(fn) }

// SetStatusQuota wires the usage lines into /status, the same ones the
// dashboard shows.
func (b *Bridge) SetStatusQuota(q *Quota) { b.in.SetQuota(q) }

// SetSettle overrides the screen and command settle delays (tests).
func (b *Bridge) SetSettle(d time.Duration) {
	b.out.deb.delay = d
	b.in.deb.delay = d
}

// Fatal delivers the first fatal Telegram error met by a job.
func (b *Bridge) Fatal() <-chan error { return b.fatal }

// Submit is a convenience for producers without their own context. Production
// producers should use SubmitContext so shutdown can release blocked sends.
func (b *Bridge) Submit(job any) {
	if err := b.SubmitContext(context.Background(), job); err != nil {
		b.log.Warn("[FIX] bridge job not accepted", slog.String("err", err.Error()))
	}
}

// SubmitContext accepts all known work with cancellable backpressure.
// State and control have separate bounded queues; neither may be lost.
func (b *Bridge) SubmitContext(ctx context.Context, job any) error {
	if e, ok := job.(domain.PrivateMessage); ok {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case b.privateJobs <- e:
			return nil
		default:
			if b.PrivateBusy != nil {
				b.PrivateBusy(e.Contact.ActorID)
			}
			b.log.Warn("private bridge ingress busy", "recipient_id", e.Contact.ActorID)
			return nil
		}
	}
	var queue chan any
	switch job.(type) {
	case AgentEvent:
		queue = b.jobs
	case ownerReplyResult, privateResult, domain.TopicMessage, domain.TopicAttachment, domain.ButtonPressed, domain.GeneralCommand, domain.StrangerSeen, presenceAway, startResult, inboxResult, transferResult, shellResult, shellAck, shellProgress, shellStatusResult, updateCheckResult, updateStartResult, herdrServerResult:
		queue = b.control
	default:
		b.log.Warn("bridge job of unknown type dropped", slog.String("type", fmt.Sprintf("%T", job)))
		return fmt.Errorf("bridge job of unknown type: %T", job)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case queue <- job:
		b.log.Debug("[FIX] bridge job accepted", slog.String("type", fmt.Sprintf("%T", job)))
		return nil
	}
}

// Dropped is retained for the daemon's legacy health metric. Reliable ingress
// no longer discards known jobs, so it remains zero.
func (b *Bridge) Dropped() int64 { return b.dropped.Load() }

// Handled returns how many jobs the bridge has taken off its queue. A
// test that floods the queue waits on it between bursts to avoid measuring
// scheduler timing instead of bridge behavior.
func (b *Bridge) Handled() int64 { return b.handled.Load() }

// Run serves jobs, screen settle timers and command follow-up timers until
// ctx is done. Errors never end the loop; fatal ones are reported through
// Fatal and the daemon decides.
func (b *Bridge) Run(ctx context.Context) {
	b.log.Info("bridge started")
	if b.pendingUpdate != nil {
		job := *b.pendingUpdate
		b.run(ctx, "update_resume", func(ctx context.Context) error { return b.in.panel.restoreUpdate(ctx, job) })
	}
	defer func() {
		b.log.Info("[FIX] bridge stopped", slog.Int64("handled", b.handled.Load()), slog.Int64("dropped", b.dropped.Load()))
	}()
	b.runCtx = ctx
	b.out.replyEnqueue = b.enqueueOwnerReply
	b.wg.Add(1)
	go func() { defer b.wg.Done(); b.runOwnerReplies(ctx) }()
	b.wg.Add(1)
	go func() { defer b.wg.Done(); b.runPromptNotices(ctx) }()
	defer b.wg.Wait()
	var privateTick <-chan time.Time
	if b.PrivateControl != nil {
		privateTick = b.in.clock.After(time.Second)
	}
	controlBurst := 0
	for {
		// Bound consecutive control jobs. A ready state event or timer gets
		// a chance after four controls, while commands remain FIFO.
		if controlBurst < 4 {
			select {
			case <-ctx.Done():
				return
			case job := <-b.control:
				b.handle(ctx, job)
				b.handled.Add(1)
				controlBurst++
				continue
			default:
			}
		}
		if controlBurst >= 4 {
			select {
			case <-ctx.Done():
				return
			case job := <-b.jobs:
				b.handle(ctx, job)
				b.handled.Add(1)
				controlBurst = 0
				continue
			case key := <-b.out.Due():
				b.run(ctx, "screen", func(ctx context.Context) error { return b.out.Fire(ctx, key) })
				controlBurst = 0
				continue
			case key := <-b.in.Due():
				b.run(ctx, "command", func(ctx context.Context) error { return b.in.Fire(ctx, key) })
				controlBurst = 0
				continue
			case key := <-b.out.TurnDue():
				b.run(ctx, "turn", func(ctx context.Context) error { return b.out.EndTurn(ctx, key) })
				controlBurst = 0
				continue
			default:
			}
		}
		select {
		case <-ctx.Done():
			return
		case job := <-b.control:
			b.handle(ctx, job)
			b.handled.Add(1)
			controlBurst++
		case job := <-b.jobs:
			b.handle(ctx, job)
			b.handled.Add(1)
			controlBurst = 0
		case key := <-b.out.Due():
			b.run(ctx, "screen", func(ctx context.Context) error { return b.out.Fire(ctx, key) })
			controlBurst = 0
		case key := <-b.in.Due():
			b.run(ctx, "command", func(ctx context.Context) error { return b.in.Fire(ctx, key) })
			controlBurst = 0
		case job := <-b.privateJobs:
			b.handle(ctx, job)
			b.handled.Add(1)
			controlBurst = 0
		case <-privateTick:
			privateTick = b.in.clock.After(time.Second)
			b.run(ctx, "private_tick", b.PrivateControl.Tick)
		case key := <-b.out.TurnDue():
			b.run(ctx, "turn", func(ctx context.Context) error { return b.out.EndTurn(ctx, key) })
			controlBurst = 0
		}
	}
}

func (b *Bridge) handle(ctx context.Context, job any) {
	switch j := job.(type) {
	case ownerReplyResult:
		<-b.ownerReplySlots
		if j.err != nil {
			b.quarantineOwnerReply(j)
			b.run(ctx, "owner_reply", func(context.Context) error { return j.err })
			return
		}
		if j.reply.hash == "" {
			return
		}
		if b.out.queuedReplies[j.reply.key] == j.reply.hash && b.out.replyRevision[j.reply.key] == j.reply.revision {
			delete(b.out.queuedReplies, j.reply.key)
			if j.reply.generation.ctx.Err() == nil {
				b.out.lastPosted[j.reply.key] = j.reply.hash
			}
		}
	case privateResult:
		if j.run != nil {
			b.run(ctx, "private_result", j.run)
		}
	case AgentEvent:
		b.log.Debug("bridge job", slog.String("kind", "agent_event"), slog.String("event", string(j.Kind)), slog.String("key", j.Agent.Key.String()))
		b.out.Observe(j)
		if b.PrivateControl != nil && b.PrivateControl.Output != nil {
			b.PrivateControl.Output.Observe(j)
		}
		if b.PrivateReconciler != nil {
			b.run(ctx, "private_lifecycle", func(ctx context.Context) error { return b.PrivateReconciler.Observe(ctx, j) })
		}
		if j.ReassociatedFrom != nil {
			b.in.Reassociate(*j.ReassociatedFrom, j.Agent.Key)
		}
		if j.Kind == AgentGone {
			b.in.Forget(j.Agent.Key)
			b.run(ctx, "forget", func(ctx context.Context) error { return b.out.Forget(ctx, j.Agent.Key) })
		}
	case domain.ButtonPressed:
		if b.Shares != nil && strings.HasPrefix(j.Data, "sh:") {
			b.run(ctx, "share_panel", func(ctx context.Context) error { return b.Shares.Press(ctx, j) })
			return
		}
		b.log.Debug("bridge job", slog.String("kind", "button"), slog.Int("thread_id", j.ThreadID), slog.Int("message_id", j.MessageID))
		if strings.HasPrefix(j.Data, panelPrefix) {
			b.run(ctx, "options_button", func(ctx context.Context) error { return b.in.PressPanel(ctx, j) })
			return
		}
		if strings.HasPrefix(j.Data, closePrefix) {
			b.run(ctx, "close_button", func(ctx context.Context) error { return b.in.PressClose(ctx, j) })
			return
		}
		if b.PrivateControl != nil {
			if key, ok := b.out.topics.KeyForThread(j.ThreadID); ok {
				_ = b.PrivateControl.InvalidatePrivate(ctx, key)
			}
		}
		b.run(ctx, "button", func(ctx context.Context) error { return b.out.Press(ctx, j) })
	case domain.PrivateMessage:
		if b.PrivateHandler != nil {
			b.run(ctx, "private_message", func(ctx context.Context) error { return b.PrivateHandler(ctx, j) })
		}
	case domain.TopicMessage:
		if b.sharingCommand(ctx, j.FromID, j.ChatID, j.ThreadID, j.MessageID, j.Text) {
			return
		}
		b.log.Debug("bridge job", slog.String("kind", "topic_message"), slog.Int("thread_id", j.ThreadID), slog.Int("message_id", j.MessageID))
		if b.PrivateControl != nil {
			cmd := domain.ParseCommand(j.Text, "")
			action, ok := domain.ShareCommandAction(cmd)
			if ok && action != domain.ShareScreen && action != domain.ShareHistory && action != domain.ShareOverview && action != domain.ShareGit {
				if key, ok := b.out.topics.KeyForThread(j.ThreadID); ok {
					_ = b.PrivateControl.InvalidatePrivate(ctx, key)
				}
			}
		}
		b.run(ctx, "topic_message", func(ctx context.Context) error { return b.in.HandleTopic(ctx, j) })
	case domain.TopicAttachment:
		b.log.Debug("bridge job", slog.String("kind", "topic_attachment"), slog.Int("thread_id", j.ThreadID), slog.Int("message_id", j.MessageID),
			slog.String("attachment", string(j.Kind)), slog.Int64("size", j.Size), slog.String("group", j.GroupID))
		b.run(ctx, "topic_attachment", func(ctx context.Context) error { return b.in.HandleAttachment(ctx, j) })
	case inboxResult:
		b.log.Debug("bridge job", slog.String("kind", "inbox_result"), slog.Int("message_id", j.messageID), slog.Int("saved", len(j.paths)), slog.Int("failed", len(j.failed)))
		b.run(ctx, "inbox_result", func(ctx context.Context) error { return b.in.InboxFinished(ctx, j) })
	case transferResult:
		b.run(ctx, "transfer_result", func(ctx context.Context) error { return b.in.transferFinished(ctx, j) })
	case shellResult:
		b.run(ctx, "shell_result", func(ctx context.Context) error { return b.in.shellFinished(ctx, j) })
	case shellAck:
		b.run(ctx, "shell_ack", func(ctx context.Context) error { return b.in.shellAckFinished(ctx, j) })
	case shellProgress:
		b.run(ctx, "shell_progress", func(ctx context.Context) error { return b.in.shellProgressFinished(ctx, j) })
	case shellStatusResult:
		b.run(ctx, "shell_status", func(ctx context.Context) error { return b.in.shellStatusFinished(ctx, j) })
	case domain.GeneralCommand:
		if b.sharingCommand(ctx, j.FromID, j.ChatID, 0, j.MessageID, j.Text) {
			return
		}
		b.log.Debug("bridge job", slog.String("kind", "general_command"), slog.Int("message_id", j.MessageID), slog.String("role", j.Role.String()))
		parsed := domain.ParseCommand(j.Text, b.in.cfg.BotUsername)
		if parsed.Kind == domain.CmdHerdrStop || parsed.Kind == domain.CmdHerdrStart {
			b.run(ctx, "herdr_server_ack", func(ctx context.Context) error {
				return b.in.HandleHerdrServer(ctx, j, parsed.Kind == domain.CmdHerdrStop)
			})
			return
		}
		b.run(ctx, "general_command", func(ctx context.Context) error { return b.in.HandleGeneral(ctx, j) })
	case domain.StrangerSeen:
		b.log.Debug("bridge job", slog.String("kind", "stranger_seen"), slog.Int64("from_id", j.FromID), slog.Int("thread_id", j.ThreadID))
		b.in.HandleStranger(j)
	case presenceAway:
		b.log.Debug("bridge job", slog.String("kind", "catch_up"))
		b.run(ctx, "catch_up", b.out.CatchUp)
	case startResult:
		b.log.Debug("bridge job", slog.String("kind", "start_result"), slog.Int("message_id", j.messageID), slog.Bool("ok", j.err == nil))
		b.run(ctx, "start_result", func(ctx context.Context) error { return b.in.StartFinished(ctx, j) })
	case updateCheckResult:
		b.run(ctx, "update_check_result", func(ctx context.Context) error { return b.in.panel.checkFinished(ctx, j) })
	case updateStartResult:
		b.run(ctx, "update_start_result", func(ctx context.Context) error { return b.in.panel.startFinished(ctx, j) })
	case herdrServerResult:
		b.in.herdrServerBusy = false
		text := "✅ Herdr server is running. The Telegram listener will reconnect to it."
		if j.stop {
			text = "✅ Herdr server stopped. All Herdr agents and processes have been terminated; previous sessions are not guaranteed to resume."
		}
		if j.err != nil {
			text = "⚠️ Herdr server control failed: " + j.err.Error()
		}
		b.run(ctx, "herdr_server_result", func(ctx context.Context) error {
			return b.in.reply(ctx, 0, j.messageID, text)
		})
	}
}

type herdrServerResult struct {
	messageID int
	stop      bool
	err       error
}

// run executes one job under the call timeout and applies the error policy.
func (b *Bridge) run(ctx context.Context, kind string, fn func(context.Context) error) {
	b.runWithTimeout(ctx, kind, b.CallTimeout, fn)
}

func (b *Bridge) runWithTimeout(ctx context.Context, kind string, timeout time.Duration, fn func(context.Context) error) {
	jctx := ctx
	cancel := func() {}
	if timeout > 0 {
		jctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		jctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	start := time.Now()
	err := fn(jctx)
	b.log.Debug("bridge job done", slog.String("kind", kind), slog.Int64("dur_ms", time.Since(start).Milliseconds()), slog.Bool("ok", err == nil))
	if err == nil {
		return
	}
	if isFatal(err) {
		select {
		case b.fatal <- err:
		default:
		}
		return
	}
	b.log.Warn("bridge job failed", slog.String("kind", kind), slog.String("err", err.Error()))
}

func (b *Bridge) sharingCommand(ctx context.Context, actor, chat int64, thread, message int, text string) bool {
	if b.Shares == nil {
		return false
	}
	cmd := domain.ParseCommand(text, b.Shares.Config.BotUsername)
	if cmd.Kind != domain.CmdShare && cmd.Kind != domain.CmdShares {
		return false
	}
	b.run(ctx, "share_panel", func(ctx context.Context) error { return b.Shares.Open(ctx, actor, chat, thread, message, text) })
	return true
}

type privateResult struct{ run func(context.Context) error }

func (b *Bridge) SetPrivateControl(p *PrivateControl) {
	b.PrivateControl = p
	b.PrivateHandler = p.Handle
	// Both run on this bridge goroutine, where the owner's hold lives.
	p.HoldPicker = b.in.PickerHold
	p.ReleasePicker = b.in.ReleasePicker
	slots := make(chan struct{}, 4)
	p.Async = func(run func(context.Context) func(context.Context) error) bool {
		select {
		case slots <- struct{}{}:
		default:
			return false
		}
		b.spawn(func(ctx context.Context) {
			defer func() { <-slots }()
			result := run(ctx)
			if result != nil {
				_ = b.SubmitContext(ctx, privateResult{result})
			}
		})
		return true
	}
	p.InvalidateOwner = func(ctx context.Context, key domain.Key) error {
		kb, ok := b.out.keyboards[key]
		delete(b.out.keyboards, key)
		delete(b.out.typing, key)
		if ok {
			return b.out.tg.EditButtons(ctx, kb.messageID, nil)
		}
		return nil
	}
}
