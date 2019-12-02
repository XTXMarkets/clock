package clock

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"log"
)

// Clock represents an interface to the functions in the standard library time
// package. Two implementations are available in the clock package. The first
// is a real-time clock which simply wraps the time package's functions. The
// second is a mock clock which will only make forward progress when
// programmatically adjusted.
type Clock interface {
	After(d time.Duration) <-chan time.Time
	AfterFunc(d time.Duration, f func()) *Timer
	Now() time.Time
	Since(t time.Time) time.Duration
	Sleep(d time.Duration)
	Tick(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) *Ticker
	NewTimer(d time.Duration) *Timer
}

// New returns an instance of a real-time clock.
func RealClock() Clock {
	return &real{}
}

// clock implements a real-time clock by simply wrapping the time package functions.
type real struct{}

func (c *real) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (c *real) AfterFunc(d time.Duration, f func()) *Timer {
	return &Timer{timer: time.AfterFunc(d, f)}
}

func (c *real) Now() time.Time { return time.Now() }

func (c *real) Since(t time.Time) time.Duration { return time.Since(t) }

func (c *real) Sleep(d time.Duration) { time.Sleep(d) }

func (c *real) Tick(d time.Duration) <-chan time.Time { return time.Tick(d) }

func (c *real) NewTicker(d time.Duration) *Ticker {
	t := time.NewTicker(d)
	return &Ticker{C: t.C, ticker: t}
}

func (c *real) NewTimer(d time.Duration) *Timer {
	t := time.NewTimer(d)
	return &Timer{C: t.C, timer: t}
}

// Mock represents a mock clock that only moves forward programmically.
// It can be preferable to a real-time clock when testing time-based functionality.
type Mock struct {
	mu     sync.Mutex
	now    time.Time  // current time
	timers clockUnits // tickers & timers
}

// NewMock returns an instance of a mock clock.
// The current time of the mock clock on initialization is the Unix epoch.
func NewMock(sec, nsec int64) *Mock {
	return &Mock{now: time.Unix(sec, nsec)}
}

// Add moves the current time of the mock clock forward by the duration.
// This should only be called from a single goroutine at a time.
func (m *Mock) Add(d time.Duration) {
	// Calculate the final current time.
	t := m.now.Add(d)

	// Continue to execute timers until there are no more before the new time.
	for {
		if !m.runNextTimer(t) {
			break
		}
	}

	// Ensure that we end with the new time.
	m.mu.Lock()
	m.now = t
	m.mu.Unlock()
}

// Set sets the current time of the mock clock to a specific one.
// This should only be called from a single goroutine at a time.
func (m *Mock) Set(t time.Time) {
	// Continue to execute timers until there are no more before the new time.
	for {
		if !m.runNextTimer(t) {
			break
		}
	}

	// Ensure that we end with the new time.
	m.mu.Lock()
	m.now = t
	m.mu.Unlock()
}

// runNextTimer executes the next timer in chronological order and moves the
// current time to the timer's next tick time. The next time is not executed if
// it's next time if after the max time. Returns true if a timer is executed.
func (m *Mock) runNextTimer(max time.Time) bool {
	m.mu.Lock()

	// Sort timers by time.
	sort.Sort(m.timers)

	// If we have no more timers then exit.
	if len(m.timers) == 0 {
		m.mu.Unlock()
		return false
	}

	// Retrieve next timer. Exit if next tick is after new time.
	t := m.timers[0]
	if t.Next().After(max) {
		m.mu.Unlock()
		return false
	}

	// Move "now" forward and unlock clock.
	m.now = t.Next()
	m.mu.Unlock()

	// Execute timer.
	t.Tick(m.now)
	return true
}

// After waits for the duration to elapse and then sends the current time on the returned channel.
func (m *Mock) After(d time.Duration) <-chan time.Time {
	return m.NewTimer(d).C
}

// AfterFunc waits for the duration to elapse and then executes a function.
// A Timer is returned that can be stopped.
func (m *Mock) AfterFunc(d time.Duration, f func()) *Timer {
	t := m.NewTimer(d)
	t.C = nil
	t.fn = f
	return t
}

// Now returns the current wall time on the mock clock.
func (m *Mock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// Since returns time since the mock clocks wall time.
func (m *Mock) Since(t time.Time) time.Duration {
	return m.Now().Sub(t)
}

// Sleep pauses the goroutine for the given duration on the mock clock.
// The clock must be moved forward in a separate goroutine.
func (m *Mock) Sleep(d time.Duration) {
	<-m.After(d)
}

// Tick is a convenience function for Ticker().
// It will return a ticker channel that cannot be stopped.
func (m *Mock) Tick(d time.Duration) <-chan time.Time {
	return m.NewTicker(d).C
}

func (m *Mock) removeClockTimer(t clockUnit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, timer := range m.timers {
		if timer == t {
			copy(m.timers[i:], m.timers[i+1:])
			m.timers[len(m.timers)-1] = nil
			m.timers = m.timers[:len(m.timers)-1]
			break
		}
	}
	sort.Sort(m.timers)
}

// Ticker creates a new instance of Ticker.
func (m *Mock) NewTicker(d time.Duration) *Ticker {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan time.Time)
	t := &Ticker{
		C:    ch,
		c:    ch,
		mock: m,
		d:    d,
		next: m.now.Add(d),

		pendingLock: make(chan struct{}, 1),
	}
	m.timers = append(m.timers, (*internalTicker)(t))
	return t
}

// Timer creates a new instance of Timer.
func (m *Mock) NewTimer(d time.Duration) *Timer {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan time.Time)
	t := &Timer{
		C:    ch,
		c:    ch,
		mock: m,
		next: m.now.Add(d),

		pendingLock: make(chan struct{}, 1),
	}
	m.timers = append(m.timers, (*internalTimer)(t))
	return t
}

// clockUnit represents an object with an associated start time.
type clockUnit interface {
	Next() time.Time
	Tick(time.Time)
}

// clockUnits represents a list of sortable timers.
type clockUnits []clockUnit

// Timer represents a single event.
// The current time will be sent on C, unless the timer was created by AfterFunc.
type Timer struct {
	C       <-chan time.Time
	c       chan time.Time
	timer   *time.Timer // realtime impl, if set
	mock    *Mock       // mock clock, if set
	fn      func()      // AfterFunc function, if set
	stopped atomicBool  // True if stopped, false if running

	nextLock sync.Mutex
	next     time.Time // next tick time

	pendingLock chan struct{}
}

// Stop turns off the ticker.
func (t *Timer) Stop() bool {
	if t.timer != nil {
		return t.timer.Stop()
	}

	select {
	case t.pendingLock <- struct{}{}:
		<-t.pendingLock
	case <-time.After(generalTimeOut): // Timeout because we might be quicker than the ticking go routine, if we call it from the receiver.
		// Some go routine waiting to write.
		log.Printf("Tick %v dropped when stopping.\n", <-t.C)
	}

	registered := !t.stopped.Load()
	t.mock.removeClockTimer((*internalTimer)(t))
	t.next = time.Time{} // If we stop the timer, we don't want to have the next tick as an offset on the last tick.
	t.stopped.Store(true)
	return registered
}

// Reset changes the expiry time of the timer
func (t *Timer) Reset(d time.Duration) bool {
	if t.timer != nil {
		return t.timer.Reset(d)
	}

	select {
	case t.pendingLock <- struct{}{}:
		<-t.pendingLock
	case <-time.After(generalTimeOut): // Timeout because we might be quicker than the ticking go routine, if we call it from the receiver.
		// Some go routine waiting to write.
		log.Printf("Tick %v dropped when resetting.\n", <-t.C)
	}

	// When we reset the timer we don't want to go off 'now', because the mock clock is ticking quite fast and might already have advanced,
	// since the timer has expired. We add to our last tick to have reliable offsets. Code in tests shouldn't have a long realtime execution
	// duration, and therefore we can assume that a timer reset happens never or immediately. This is an assumption we need to make for a
	// reliable mock clock.
	t.nextLock.Lock()
	if !t.next.IsZero() {
		t.next = t.next.Add(d)
	} else {
		t.next = t.mock.Now().Add(d)
	}
	t.nextLock.Unlock()

	registered := !t.stopped.Load()
	if !registered {
		t.mock.mu.Lock()
		t.mock.timers = append(t.mock.timers, (*internalTimer)(t))
		t.mock.mu.Unlock()
	}
	t.stopped.Store(false)
	return registered
}

type internalTimer Timer

func (t *internalTimer) Next() time.Time {
	t.nextLock.Lock()
	defer t.nextLock.Unlock()
	return t.next
}

func (t *internalTimer) Tick(now time.Time) {
	t.mock.removeClockTimer((*internalTimer)(t))
	t.stopped.Store(true)

	select {
	case t.pendingLock <- struct{}{}:
	default:
		// Some go routine already trying to write to channel.
		log.Printf("Tick at %v dropped.\n", now)
		return
	}

	if t.fn != nil {
		t.fn()
		<-t.pendingLock
		return
	}

	select {
	case t.c <- now: // This channel blocks.
		<-t.pendingLock
		return
	case <-time.After(channelTimeOut):
		log.Printf("Tick timeout for %v.\n", now)
	}

	// We have no pending go routine and the consumer doesn't consume our tick. We spin of a go routine that blocks on the channel after the
	// consumer consumes. If the consumer is dead, we're leaking a go routine here, but that seems okay, since we're only using the mock
	// clock in tests.

	go func() {
		defer func() { <-t.pendingLock }()
		t.c <- now
	}()
}

func (a clockUnits) Len() int           { return len(a) }
func (a clockUnits) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a clockUnits) Less(i, j int) bool { return a[i].Next().Before(a[j].Next()) }

const (
	// Timeout after that we move the blocking channel into a go routine, and continue processing.
	channelTimeOut = 100 * time.Millisecond
	// Timeout for operations that we never expect to block for long. We have a rather long timeout to avoid flakiness.
	generalTimeOut = 4 * time.Second
)

// Ticker holds a channel that receives "ticks" at regular intervals.
type Ticker struct {
	C      <-chan time.Time
	c      chan time.Time
	ticker *time.Ticker  // realtime impl, if set
	mock   *Mock         // mock clock, if set
	d      time.Duration // time between ticks

	nextLock sync.Mutex
	next     time.Time // next tick time

	pendingLock chan struct{}
}

type internalTicker Ticker

// Stop turns off the ticker.
func (t *Ticker) Stop() {
	if t.ticker != nil {
		t.ticker.Stop()
	} else {
		t.mock.removeClockTimer((*internalTicker)(t))
	}
}

func (t *internalTicker) Next() time.Time {
	t.nextLock.Lock()
	defer t.nextLock.Unlock()
	return t.next
}

func (t *internalTicker) Tick(now time.Time) {
	t.nextLock.Lock()
	t.next = now.Add(t.d)
	t.nextLock.Unlock()

	select {
	case t.pendingLock <- struct{}{}:
	default:
		// Some go routine already trying to write to channel.
		log.Printf("Tick at %v dropped.\n", now)
		return
	}

	select {
	case t.c <- now: // This channel blocks.
		<-t.pendingLock
		return
	case <-time.After(channelTimeOut):
		log.Printf("Tick timeout for %v.\n", now)
	}

	// We have no pending go routine and the consumer doesn't consume our tick. We spin of a go routine that blocks on the channel after the
	// consumer consumes. If the consumer is dead, we're leaking a go routine here, but that seems okay, since we're only using the mock
	// clock in tests.

	go func() {
		defer func() { <-t.pendingLock }()
		t.c <- now
	}()
}

type atomicBool struct{ value int32 }

func (v *atomicBool) Load() bool {
	return atomic.LoadInt32(&v.value) != 0
}

func (v *atomicBool) Store(n bool) {
	var i int32
	if n {
		i = 1
	}
	atomic.StoreInt32(&v.value, i)
}
