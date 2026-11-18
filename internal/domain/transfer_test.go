package domain

import (
	"reflect"
	"testing"
)

func TestTransferCommandRouting(t *testing.T) {
	got := Route(`/transfer /tmp/a "/tmp/my file"`, "bot", StatusIdle)
	if got.Kind != CmdTransfer || !reflect.DeepEqual(got.TransferPaths, []string{"/tmp/a", "/tmp/my file"}) {
		t.Fatalf("Route transfer = %+v", got)
	}
	if got := ParseCommand(`/transfer@other /tmp/a`, "bot"); got.Kind != CmdUnknown {
		t.Fatalf("other bot command = %+v", got)
	}
	if got := ParseCommand(`/transfer`, "bot"); got.Kind != CmdUnknown {
		t.Fatalf("empty transfer = %+v", got)
	}
}

func TestParseTransferPathsRejectsUnclosedQuote(t *testing.T) {
	if _, err := ParseTransferPaths(`"/tmp/missing`); err == nil {
		t.Fatal("unterminated quote accepted")
	}
}
