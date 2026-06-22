//go:build integration

package tests

import (
	"os"
	"testing"

	"github.com/lkmavi/broker/tests/suite"
)

func TestMain(m *testing.M) {
	stop := suite.Start()
	code := m.Run()
	stop()
	os.Exit(code)
}
