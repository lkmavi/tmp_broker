//go:build integration

package tests

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lkmavi/broker/tests/suite"
)

func TestPut_OK(t *testing.T) {
	s := suite.New(t)
	resp := s.PUT(t, "/pet?v=cat")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
}

func TestPut_MissingValue(t *testing.T) {
	s := suite.New(t)
	resp := s.PUT(t, "/pet")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("want 400, got %d", resp.StatusCode)
	}
}

func TestPut_EmptyQueueName(t *testing.T) {
	s := suite.New(t)
	resp := s.PUT(t, "/?v=x")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("want 400, got %d", resp.StatusCode)
	}
}

func TestGet_FIFO(t *testing.T) {
	s := suite.New(t)
	s.PUT(t, "/pet?v=cat").Body.Close()
	s.PUT(t, "/pet?v=dog").Body.Close()

	check := func(want string, wantStatus int) {
		t.Helper()
		resp := s.GET(t, "/pet")
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != wantStatus {
			t.Errorf("status: want %d, got %d", wantStatus, resp.StatusCode)
		}
		if string(b) != want {
			t.Errorf("body: want %q, got %q", want, string(b))
		}
	}

	check("cat", http.StatusOK)
	check("dog", http.StatusOK)
	check("", http.StatusNotFound)
}

func TestGet_Empty_NoTimeout(t *testing.T) {
	s := suite.New(t)
	resp := s.GET(t, "/empty")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("want 404, got %d", resp.StatusCode)
	}
}

func TestGet_EmptyQueueName(t *testing.T) {
	s := suite.New(t)
	resp := s.GET(t, "/")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("want 400, got %d", resp.StatusCode)
	}
}

func TestGet_Timeout_Expires(t *testing.T) {
	s := suite.New(t)
	resp := s.GET(t, "/empty?timeout=1")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("want 404, got %d", resp.StatusCode)
	}
}

func TestGet_Timeout_MessageArrives(t *testing.T) {
	s := suite.New(t)
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.PUT(t, "/pet?v=cat").Body.Close()
	}()

	resp := s.GET(t, "/pet?timeout=5")
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	if string(b) != "cat" {
		t.Errorf("want %q, got %q", "cat", string(b))
	}
}

// TestGet_WaiterFIFO verifies that waiting consumers receive messages
// in the same order they registered their requests.
func TestGet_WaiterFIFO(t *testing.T) {
	s := suite.New(t)
	results := make([]string, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	// Use direct HTTP calls inside goroutines to avoid t.Fatal across goroutine boundary.
	makeGet := func(path string) string {
		req, _ := http.NewRequest(http.MethodGet, s.URL()+path, nil)
		resp, err := s.Client().Do(req)
		if err != nil {
			return ""
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return string(b)
	}

	go func() {
		defer wg.Done()
		results[0] = makeGet("/fifo?timeout=5")
	}()
	time.Sleep(100 * time.Millisecond) // ensure first waiter registers before second

	go func() {
		defer wg.Done()
		results[1] = makeGet("/fifo?timeout=5")
	}()
	time.Sleep(100 * time.Millisecond) // ensure both waiters are registered

	s.PUT(t, "/fifo?v=first").Body.Close()
	s.PUT(t, "/fifo?v=second").Body.Close()
	wg.Wait()

	if results[0] != "first" {
		t.Errorf("first waiter: want %q, got %q", "first", results[0])
	}
	if results[1] != "second" {
		t.Errorf("second waiter: want %q, got %q", "second", results[1])
	}
}

func TestGet_SeparateQueues(t *testing.T) {
	s := suite.New(t)
	s.PUT(t, "/queue-a?v=aaa").Body.Close()
	s.PUT(t, "/queue-b?v=bbb").Body.Close()

	ra := s.GET(t, "/queue-a")
	ba, _ := io.ReadAll(ra.Body)
	ra.Body.Close()
	if string(ba) != "aaa" {
		t.Errorf("queue-a: want %q, got %q", "aaa", string(ba))
	}

	rb := s.GET(t, "/queue-b")
	bb, _ := io.ReadAll(rb.Body)
	rb.Body.Close()
	if string(bb) != "bbb" {
		t.Errorf("queue-b: want %q, got %q", "bbb", string(bb))
	}
}

// TestGet_MultipleQueuesIndependent verifies the example from the spec verbatim.
func TestGet_MultipleQueuesIndependent(t *testing.T) {
	s := suite.New(t)

	s.PUT(t, "/pet?v=cat").Body.Close()
	s.PUT(t, "/pet?v=dog").Body.Close()
	s.PUT(t, "/role?v=manager").Body.Close()
	s.PUT(t, "/role?v=executive").Body.Close()

	readBody := func(path string) (int, string) {
		resp := s.GET(t, path)
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if code, body := readBody("/pet"); code != 200 || body != "cat" {
		t.Errorf("pet[0]: want 200/cat, got %d/%q", code, body)
	}
	if code, body := readBody("/pet"); code != 200 || body != "dog" {
		t.Errorf("pet[1]: want 200/dog, got %d/%q", code, body)
	}
	if code, _ := readBody("/pet"); code != 404 {
		t.Errorf("pet[2]: want 404, got %d", code)
	}
	if code, body := readBody("/role"); code != 200 || body != "manager" {
		t.Errorf("role[0]: want 200/manager, got %d/%q", code, body)
	}
	if code, body := readBody("/role"); code != 200 || body != "executive" {
		t.Errorf("role[1]: want 200/executive, got %d/%q", code, body)
	}
	if code, _ := readBody("/role"); code != 404 {
		t.Errorf("role[2]: want 404, got %d", code)
	}
}

// TestPut_EmptyValueIsValid verifies that ?v= (present but empty) is accepted —
// the spec forbids only absent v, not an empty string value.
func TestPut_EmptyValueIsValid(t *testing.T) {
	s := suite.New(t)

	r1 := s.PUT(t, "/q?v=")
	r1.Body.Close()
	if r1.StatusCode != http.StatusOK {
		t.Errorf("PUT ?v=: want 200, got %d", r1.StatusCode)
	}

	r2 := s.GET(t, "/q")
	defer r2.Body.Close()
	b, _ := io.ReadAll(r2.Body)
	if r2.StatusCode != http.StatusOK {
		t.Errorf("GET after empty PUT: want 200, got %d", r2.StatusCode)
	}
	if string(b) != "" {
		t.Errorf("body: want empty string, got %q", string(b))
	}
}

// TestGet_Timeout_Zero verifies that timeout=0 is treated as no timeout (immediate 404).
func TestGet_Timeout_Zero(t *testing.T) {
	s := suite.New(t)
	resp := s.GET(t, "/q?timeout=0")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("want 404, got %d", resp.StatusCode)
	}
}

// TestGet_Timeout_Negative verifies that a negative timeout returns 404 immediately.
func TestGet_Timeout_Negative(t *testing.T) {
	s := suite.New(t)
	resp := s.GET(t, "/q?timeout=-1")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("want 404, got %d", resp.StatusCode)
	}
}

// TestGet_Timeout_NonNumeric verifies that a non-numeric timeout (parsed as 0)
// returns 404 immediately.
func TestGet_Timeout_NonNumeric(t *testing.T) {
	s := suite.New(t)
	resp := s.GET(t, "/q?timeout=abc")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("want 404, got %d", resp.StatusCode)
	}
}

// TestMethod_NotAllowed verifies that unsupported HTTP methods return 405.
func TestMethod_NotAllowed(t *testing.T) {
	s := suite.New(t)
	req, _ := http.NewRequest(http.MethodDelete, s.URL()+"/q", nil)
	resp, err := s.Client().Do(req)
	if err != nil {
		t.Fatalf("DELETE /q: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("want 405, got %d", resp.StatusCode)
	}
}

// TestGet_MessageWithSpecialChars verifies that URL-encoded characters in the
// value are decoded and stored correctly.
func TestGet_MessageWithSpecialChars(t *testing.T) {
	s := suite.New(t)
	s.PUT(t, "/q?v=hello%20world").Body.Close()

	resp := s.GET(t, "/q")
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	if string(b) != "hello world" {
		t.Errorf("want %q, got %q", "hello world", string(b))
	}
}

// TestGet_AfterWaiterExpires verifies that once a waiter times out and removes
// itself, subsequent PUT+GET operations on the same queue work correctly.
func TestGet_AfterWaiterExpires(t *testing.T) {
	s := suite.New(t)

	expired := s.GET(t, "/q?timeout=1")
	expired.Body.Close()
	if expired.StatusCode != http.StatusNotFound {
		t.Errorf("expired waiter: want 404, got %d", expired.StatusCode)
	}

	s.PUT(t, "/q?v=cat").Body.Close()
	r := s.GET(t, "/q")
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	if r.StatusCode != http.StatusOK || string(b) != "cat" {
		t.Errorf("after expiry: want 200/cat, got %d/%q", r.StatusCode, string(b))
	}
}

// TestPut_DispatchesToWaiterNotQueue verifies that when a consumer is waiting,
// the message is delivered directly to it and the queue remains empty afterward.
func TestPut_DispatchesToWaiterNotQueue(t *testing.T) {
	s := suite.New(t)
	received := make(chan string, 1)

	go func() {
		req, _ := http.NewRequest(http.MethodGet, s.URL()+"/q?timeout=5", nil)
		resp, err := s.Client().Do(req)
		if err != nil {
			received <- ""
			return
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		received <- string(b)
	}()
	time.Sleep(100 * time.Millisecond) // ensure waiter is registered

	s.PUT(t, "/q?v=direct").Body.Close()

	if got := <-received; got != "direct" {
		t.Errorf("waiter: want %q, got %q", "direct", got)
	}
	// message went straight to the waiter — queue must still be empty
	r := s.GET(t, "/q")
	r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Errorf("queue after direct dispatch: want 404, got %d", r.StatusCode)
	}
}

// TestGet_WaiterFIFO_Three extends the FIFO waiter test to three concurrent consumers.
func TestGet_WaiterFIFO_Three(t *testing.T) {
	s := suite.New(t)
	results := make([]string, 3)
	var wg sync.WaitGroup
	wg.Add(3)

	makeGet := func(path string) string {
		req, _ := http.NewRequest(http.MethodGet, s.URL()+path, nil)
		resp, err := s.Client().Do(req)
		if err != nil {
			return ""
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return string(b)
	}

	for i := range 3 {
		i := i
		go func() {
			defer wg.Done()
			results[i] = makeGet("/fifo3?timeout=5")
		}()
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // ensure all three are registered

	for _, v := range []string{"first", "second", "third"} {
		s.PUT(t, "/fifo3?v="+v).Body.Close()
	}
	wg.Wait()

	for i, want := range []string{"first", "second", "third"} {
		if results[i] != want {
			t.Errorf("waiter[%d]: want %q, got %q", i, want, results[i])
		}
	}
}

// TestGet_ConcurrentPuts verifies that messages from concurrent PUTs are all
// stored and retrievable (order is not guaranteed for concurrent producers).
func TestGet_ConcurrentPuts(t *testing.T) {
	s := suite.New(t)
	const n = 5
	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		i := i
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPut,
				s.URL()+fmt.Sprintf("/q?v=msg%d", i), nil)
			resp, _ := s.Client().Do(req)
			if resp != nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	got := make(map[string]bool, n)
	for range n {
		r := s.GET(t, "/q")
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Errorf("GET: want 200, got %d", r.StatusCode)
		}
		got[string(b)] = true
	}
	for i := range n {
		key := fmt.Sprintf("msg%d", i)
		if !got[key] {
			t.Errorf("concurrent puts: message %q not found", key)
		}
	}

	r := s.GET(t, "/q")
	r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Errorf("after draining: want 404, got %d", r.StatusCode)
	}
}

// TestGet_ClientDisconnect verifies that when a waiting consumer disconnects,
// its waiter slot is cleaned up and the queue continues to work correctly.
func TestGet_ClientDisconnect(t *testing.T) {
	s := suite.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.URL()+"/q?timeout=10", nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := s.Client().Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()
	time.Sleep(100 * time.Millisecond) // ensure waiter is registered
	cancel()                           // simulate client disconnect
	<-done
	time.Sleep(50 * time.Millisecond) // allow server goroutine to process cancellation

	// Queue must accept and deliver new messages normally after cleanup.
	s.PUT(t, "/q?v=after").Body.Close()
	r := s.GET(t, "/q")
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	if r.StatusCode != http.StatusOK || string(b) != "after" {
		t.Errorf("after disconnect: want 200/after, got %d/%q", r.StatusCode, string(b))
	}
}
