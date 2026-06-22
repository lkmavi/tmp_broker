//go:build integration

package suite

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// Suite holds client config for one isolated broker process.
type Suite struct {
	client  *http.Client
	baseURL string
}

var binPath string

// Start builds the broker binary once for all tests.
// Call from TestMain; invoke the returned func to remove the binary.
func Start() func() {
	f, err := os.CreateTemp("", "broker-integ-*")
	if err != nil {
		panic("create temp file: " + err.Error())
	}
	f.Close()
	binPath = f.Name()

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = projectRoot()
	if out, err := cmd.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("build broker: %v\n%s", err, out))
	}

	return func() { os.Remove(binPath) }
}

// New starts a fresh broker process for t and registers cleanup.
// Each test gets an isolated server, so queue names need not be unique.
func New(t *testing.T) *Suite {
	t.Helper()
	port := mustFreePort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	cmd := exec.Command(binPath, strconv.Itoa(port))
	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })

	mustWaitForServer(t, addr)

	return &Suite{
		client:  &http.Client{Timeout: 30 * time.Second},
		baseURL: "http://" + addr,
	}
}

func (s *Suite) URL() string          { return s.baseURL }
func (s *Suite) Client() *http.Client { return s.client }

func (s *Suite) PUT(t *testing.T, path string) *http.Response {
	t.Helper()
	return s.do(t, http.MethodPut, path)
}

func (s *Suite) GET(t *testing.T, path string) *http.Response {
	t.Helper()
	return s.do(t, http.MethodGet, path)
}

func (s *Suite) do(t *testing.T, method, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, s.baseURL+path, nil)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func projectRoot() string {
	_, file, _, _ := runtime.Caller(0)
	// suite.go lives at tests/suite/suite.go — two dirs up is the project root
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func mustFreePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func mustWaitForServer(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("broker at %s did not start in time", addr)
}
