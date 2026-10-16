// Package dlq inspects and replays dead-lettered messages.
package dlq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/events"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/messaging"
)

// Entry is one parked message.
type Entry struct {
	Seq             uint64 `json:"seq"`
	Consumer        string `json:"consumer"`
	OriginalSubject string `json:"original_subject"`
	Attempts        int    `json:"attempts"`
	Error           string `json:"error"`
	Data            string `json:"data"`
}

// List returns every message currently in the DLQ stream (optionally filtered
// by consumer).
func List(ctx context.Context, js jetstream.JetStream, consumer string) ([]Entry, error) {
	st, err := js.Stream(ctx, events.StreamDLQ)
	if err != nil {
		return nil, err
	}
	info, err := st.Info(ctx)
	if err != nil {
		return nil, err
	}
	var out []Entry
	if info.State.Msgs == 0 {
		return out, nil
	}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		m, err := st.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue // deleted after replay
		}
		if err != nil {
			return nil, err
		}
		e := toEntry(m)
		if consumer == "" || e.Consumer == consumer {
			out = append(out, e)
		}
	}
	return out, nil
}

func toEntry(m *jetstream.RawStreamMsg) Entry {
	attempts, _ := strconv.Atoi(m.Header.Get(messaging.HeaderDLQAttempts))
	return Entry{
		Seq:             m.Sequence,
		Consumer:        m.Header.Get(messaging.HeaderDLQConsumer),
		OriginalSubject: m.Header.Get(messaging.HeaderDLQSubject),
		Attempts:        attempts,
		Error:           m.Header.Get(messaging.HeaderDLQError),
		Data:            string(m.Data),
	}
}

// Replay re-publishes a parked message to its original subject and removes it
// from the DLQ. The envelope (and therefore its event id) is unchanged, so
// consumers that already processed it will still deduplicate; only the
// JetStream Nats-Msg-Id is fresh so the publish is not swallowed by the
// stream's duplicate window.
func Replay(ctx context.Context, js jetstream.JetStream, seq uint64) error {
	st, err := js.Stream(ctx, events.StreamDLQ)
	if err != nil {
		return err
	}
	m, err := st.GetMsg(ctx, seq)
	if err != nil {
		return fmt.Errorf("dlq seq %d: %w", seq, err)
	}
	subject := m.Header.Get(messaging.HeaderDLQSubject)
	if !strings.HasPrefix(subject, events.SubjectPrefix) {
		return fmt.Errorf("dlq seq %d: missing original subject", seq)
	}
	out := nats.NewMsg(subject)
	out.Data = m.Data
	for k, vs := range m.Header {
		if strings.HasPrefix(k, "Dlq-") || k == nats.MsgIdHdr {
			continue
		}
		for _, v := range vs {
			out.Header.Add(k, v)
		}
	}
	if _, err := js.PublishMsg(ctx, out, jetstream.WithMsgID(fmt.Sprintf("replay:%d:%s", seq, m.Header.Get(messaging.HeaderDLQSeq)))); err != nil {
		return err
	}
	return st.DeleteMsg(ctx, seq)
}
