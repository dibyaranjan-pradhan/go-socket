// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func writeReport(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "index.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCheck(t *testing.T) {
	p := writeReport(t, `{"agent":{
		"1.1.1":{"behavior":"OK","behaviorClose":"OK"},
		"6.4.3":{"behavior":"NON-STRICT","behaviorClose":"OK"},
		"7.1.6":{"behavior":"INFORMATIONAL","behaviorClose":"INFORMATIONAL"},
		"5.1":{"behavior":"FAILED","behaviorClose":"OK"},
		"3.1":{"behavior":"OK","behaviorClose":"WRONG CODE"}}}`)
	failed, total, err := check(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"agent 3.1 behavior=OK close=WRONG CODE",
		"agent 5.1 behavior=FAILED close=OK",
	}
	if total != 5 || !reflect.DeepEqual(failed, want) {
		t.Fatalf("total=%d failed=%q", total, failed)
	}
}

func TestCheckErrors(t *testing.T) {
	if _, _, err := check(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("missing report accepted")
	}
	if _, _, err := check(writeReport(t, "not json")); err == nil {
		t.Fatal("malformed report accepted")
	}
}
