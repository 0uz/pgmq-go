package pgmq

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestBuildSendTopicQuery(t *testing.T) {
	msg := json.RawMessage(`{"ok":true}`)
	tests := []struct {
		name    string
		opts    sendOpts
		wantSQL string
	}{
		{name: "plain", wantSQL: "SELECT pgmq.send_topic($1, $2)"},
		{name: "delay", opts: sendOpts{delay: intPtr(5)}, wantSQL: "SELECT pgmq.send_topic($1, $2, $3::int)"},
		{name: "headers", opts: sendOpts{headers: json.RawMessage(`{"trace":"x"}`)}, wantSQL: "SELECT pgmq.send_topic($1, $2, $3::jsonb, $4::int)"},
		{name: "headers and delay", opts: sendOpts{headers: json.RawMessage(`{"trace":"x"}`), delay: intPtr(5)}, wantSQL: "SELECT pgmq.send_topic($1, $2, $3::jsonb, $4::int)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args := buildSendTopicQuery("events.created", msg, &tt.opts)
			if query != tt.wantSQL {
				t.Fatalf("query = %q, want %q", query, tt.wantSQL)
			}
			if len(args) == 0 || args[0] != "events.created" {
				t.Fatalf("unexpected query args: %#v", args)
			}
		})
	}
}

func TestBuildSendBatchTopicQuerySupportsTimestamp(t *testing.T) {
	delay := time.Now()
	opts := &sendOpts{delayTimestamp: &delay}
	query, args := buildSendBatchTopicQuery("events.created", []json.RawMessage{json.RawMessage(`{}`)}, opts)

	if !strings.Contains(query, "send_batch_topic") || !strings.Contains(query, "timestamptz") {
		t.Fatalf("query = %q, want timestamp overload", query)
	}
	if len(args) != 3 || args[0] != "events.created" {
		t.Fatalf("unexpected query args: %#v", args)
	}
}

func TestValidateTopicSendOptsRejectsTimestamp(t *testing.T) {
	delay := time.Now()
	err := validateTopicSendOpts(&sendOpts{delayTimestamp: &delay})
	if err == nil || !strings.Contains(err.Error(), "WithDelayTimestamp") {
		t.Fatalf("error = %v, want timestamp validation error", err)
	}
}

func intPtr(v int) *int { return &v }
