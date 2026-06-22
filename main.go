package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// broker is an HTTP handler that manages named in-memory FIFO message queues.
type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

// queue holds pending messages and goroutines waiting for new ones.
// msgs and waiters are mutually exclusive: if msgs is non-empty, no goroutine waits,
// and vice versa.
type queue struct {
	msgs    []string
	waiters []chan string
}

// newBroker returns a broker ready to serve requests.
func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

// ServeHTTP dispatches PUT and GET requests using the URL path as the queue name.
// Any other method returns 405; an empty path returns 400.
func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		b.put(w, r, name)
	case http.MethodGet:
		b.get(w, r, name)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// q returns the named queue, creating it on first access.
// Must be called with b.mu held.
func (b *broker) q(name string) *queue {
	if _, ok := b.queues[name]; !ok {
		b.queues[name] = &queue{}
	}
	return b.queues[name]
}

// put enqueues the value from the "v" query parameter into name.
// If a consumer is already waiting it receives the value directly instead.
// Returns 400 when the "v" parameter is absent.
func (b *broker) put(w http.ResponseWriter, r *http.Request, name string) {
	params := r.URL.Query()
	if !params.Has("v") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	v := params.Get("v")
	b.mu.Lock()
	q := b.q(name)
	if len(q.waiters) > 0 {
		ch := q.waiters[0]
		q.waiters = q.waiters[1:]
		// Send under the lock: ch has capacity 1 so this never blocks.
		// This guarantees that once get() observes ch absent from waiters,
		// the message is already buffered — safe to receive without the lock.
		ch <- v
	} else {
		q.msgs = append(q.msgs, v)
	}
	b.mu.Unlock()
}

// get dequeues and returns the next message from name.
// If the queue is empty and timeout > 0, it waits up to that many seconds for a message.
// Returns 404 on timeout. On client disconnect the waiter is removed and any
// already-dispatched message is requeued so it is not lost.
func (b *broker) get(w http.ResponseWriter, r *http.Request, name string) {
	b.mu.Lock()
	q := b.q(name)
	if len(q.msgs) > 0 {
		msg := q.msgs[0]
		q.msgs = q.msgs[1:]
		b.mu.Unlock()
		fmt.Fprint(w, msg)
		return
	}

	timeout, _ := strconv.Atoi(r.URL.Query().Get("timeout"))
	if timeout <= 0 {
		b.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}

	ch := make(chan string, 1)
	q.waiters = append(q.waiters, ch)
	b.mu.Unlock()

	// removeWaiter removes ch from q.waiters under the lock.
	// Returns true if removed (no message dispatched yet).
	// Returns false if put() already claimed ch (message is buffered in ch).
	removeWaiter := func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		for i, c := range q.waiters {
			if c == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				return true
			}
		}
		return false
	}

	timer := time.NewTimer(time.Duration(timeout) * time.Second)
	defer timer.Stop()

	select {
	case msg := <-ch:
		fmt.Fprint(w, msg)
	case <-timer.C:
		if removeWaiter() {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fmt.Fprint(w, <-ch)
	case <-r.Context().Done():
		if removeWaiter() {
			return
		}
		// put() dispatched to us before the disconnect was noticed; requeue to front.
		msg := <-ch
		b.mu.Lock()
		q.msgs = append([]string{msg}, q.msgs...)
		b.mu.Unlock()
	}
}

// main starts the broker HTTP server on the port given as the first CLI argument.
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: broker <port>")
		os.Exit(1)
	}
	if err := http.ListenAndServe(":"+os.Args[1], newBroker()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
