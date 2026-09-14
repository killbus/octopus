package relay

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/utils/log"
)

type firstTokenBudget struct {
	ctx     context.Context
	timer   *time.Timer
	cancel  context.CancelCauseFunc
	mu      sync.Mutex
	stopped bool
	once    sync.Once
}

func (b *firstTokenBudget) stopTimer() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return
	}
	b.stopped = true
	if b.timer == nil {
		return
	}
	b.timer.Stop()
}

// rearm 重建一次性首字预算定时器（空输出保持期间的 OnHeldChunk 回调，design.md 模式拆分）。
// 已停止（首字已到 / 预算已关闭）时为 no-op——重试窗口只属于未产出首字的流。
// 每次重排给满整段预算；代际守卫（b.timer != t）确保并发竞态下旧定时器的回调
// 不会在重排之后误触发 cancel。AfterFunc 定时器无公开 channel，无需 drain。
func (b *firstTokenBudget) rearm(d time.Duration) {
	if b == nil || d <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return
	}
	if b.timer != nil {
		b.timer.Stop()
	}
	var t *time.Timer
	t = time.AfterFunc(d, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.stopped || b.timer != t {
			return
		}
		b.cancel(errFirstTokenTimeout)
	})
	b.timer = t
}

func (b *firstTokenBudget) close() {
	if b == nil {
		return
	}
	b.once.Do(func() {
		b.stopTimer()
		if b.cancel != nil {
			b.cancel(context.Canceled)
		}
	})
}

func (ra *relayAttempt) attachFirstTokenBudget(req *http.Request) *http.Request {
	if req == nil || !ra.shouldUseFirstTokenBudget() {
		return req
	}

	ctx, cancel := context.WithCancelCause(req.Context())
	budget := &firstTokenBudget{ctx: ctx, cancel: cancel}
	budget.timer = time.AfterFunc(time.Duration(ra.firstTokenTimeOutSec)*time.Second, func() {
		budget.mu.Lock()
		defer budget.mu.Unlock()
		if budget.stopped {
			return
		}
		cancel(errFirstTokenTimeout)
	})
	ra.firstTokenBudget = budget
	return req.WithContext(ctx)
}

func (ra *relayAttempt) shouldUseFirstTokenBudget() bool {
	return ra != nil &&
		ra.firstTokenTimeOutSec > 0 &&
		ra.internalRequest != nil &&
		ra.internalRequest.Stream != nil &&
		*ra.internalRequest.Stream
}

func (ra *relayAttempt) stopFirstTokenTimer() {
	if ra == nil || ra.firstTokenBudget == nil {
		return
	}
	ra.firstTokenBudget.stopTimer()
}

// rearmFirstTokenTimer 空输出保持期间的 OnHeldChunk 回调：模式拆分重排首字计时。
//   - budget 模式（ra.firstTokenBudget != nil）：重建一次性 AfterFunc 预算定时器。
//   - processor-timer 模式（firstTokenBudget == nil，FirstTokenTimeout 下发到
//     StreamConfig）：由 processor 内部重置自己的定时器，这里无事可做。
func (ra *relayAttempt) rearmFirstTokenTimer() {
	if ra == nil || ra.firstTokenBudget == nil {
		return
	}
	ra.firstTokenBudget.rearm(time.Duration(ra.firstTokenTimeOutSec) * time.Second)
}

func (ra *relayAttempt) closeFirstTokenBudget() {
	if ra == nil || ra.firstTokenBudget == nil {
		return
	}
	ra.firstTokenBudget.close()
}

func (ra *relayAttempt) firstTokenTimeoutError() error {
	if ra == nil || ra.firstTokenTimeOutSec <= 0 {
		return errFirstTokenTimeout
	}
	return fmt.Errorf("%w (%ds)", errFirstTokenTimeout, ra.firstTokenTimeOutSec)
}

func (ra *relayAttempt) firstTokenTimeoutIfNeeded(ctx context.Context, err error) error {
	budgetCtx := context.Context(nil)
	if ra != nil && ra.firstTokenBudget != nil {
		budgetCtx = ra.firstTokenBudget.ctx
	}
	if isFirstTokenTimeout(ctx, err) || isFirstTokenTimeout(ctx, contextError(ctx)) ||
		isFirstTokenTimeout(budgetCtx, err) || isFirstTokenTimeout(budgetCtx, contextError(budgetCtx)) {
		if ra != nil && ra.firstTokenTimeOutSec > 0 {
			log.Warnf("first token timeout (%ds), switching channel (channel=%s, base_url_key=%s)",
				ra.firstTokenTimeOutSec, ra.channelNameForLog(), ra.baseURLKeyForLog())
		}
		return ra.firstTokenTimeoutError()
	}
	return nil
}

type closeWithFuncReadCloser struct {
	io.ReadCloser
	onClose func()
}

func (c *closeWithFuncReadCloser) Close() error {
	err := c.ReadCloser.Close()
	if c.onClose != nil {
		c.onClose()
	}
	return err
}
