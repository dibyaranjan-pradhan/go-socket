// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

// Command check fails when an Autobahn fuzzingclient report contains a failing case.
// wstest itself exits 0 regardless of results, so CI needs this gate.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
)

type result struct {
	Behavior      string `json:"behavior"`
	BehaviorClose string `json:"behaviorClose"`
}

// passing lists the Autobahn verdicts that satisfy RFC 6455; NON-STRICT and
// INFORMATIONAL mark behaviour the spec leaves to the implementation.
var passing = map[string]bool{"OK": true, "NON-STRICT": true, "INFORMATIONAL": true}

func main() {
	path := flag.String("report", "tools/autobahn/reports/index.json", "fuzzingclient index.json")
	flag.Parse()
	failed, total, err := check(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	for _, f := range failed {
		fmt.Println("FAIL", f)
	}
	fmt.Printf("autobahn: %d cases, %d failed\n", total, len(failed))
	if len(failed) > 0 || total == 0 {
		os.Exit(1)
	}
}

func check(path string) (failed []string, total int, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	var agents map[string]map[string]result
	if err := json.Unmarshal(raw, &agents); err != nil {
		return nil, 0, fmt.Errorf("parse %s: %w", path, err)
	}
	for agent, cases := range agents {
		for id, r := range cases {
			total++
			if !passing[r.Behavior] || !passing[r.BehaviorClose] {
				failed = append(failed, fmt.Sprintf("%s %s behavior=%s close=%s", agent, id, r.Behavior, r.BehaviorClose))
			}
		}
	}
	sort.Strings(failed)
	return failed, total, nil
}
