package sse_test

import (
	"testing"

	"github.com/imlargo/sse/internal/leak"
)

// RP-4: leak detection covers every test in the package.
func TestMain(m *testing.M) { leak.Main(m.Run) }
