// Command dlqctl lists and replays dead-lettered messages.
//
//	dlqctl list [-consumer inventory]
//	dlqctl replay <seq>...
//	dlqctl replay -all [-consumer inventory]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/dlq"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "dlqctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: dlqctl list|replay [flags]")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	consumer := fs.String("consumer", "", "only messages dead-lettered by this consumer")
	all := fs.Bool("all", false, "replay every listed message")
	url := fs.String("nats", envOr("NATS_URL", nats.DefaultURL), "NATS server URL")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	nc, err := nats.Connect(*url, nats.Name("dlqctl"))
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch args[0] {
	case "list":
		entries, err := dlq.List(ctx, js, *consumer)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(entries)
	case "replay":
		var seqs []uint64
		if *all {
			entries, err := dlq.List(ctx, js, *consumer)
			if err != nil {
				return err
			}
			for _, e := range entries {
				seqs = append(seqs, e.Seq)
			}
		}
		for _, a := range fs.Args() {
			s, err := strconv.ParseUint(a, 10, 64)
			if err != nil {
				return fmt.Errorf("bad sequence %q", a)
			}
			seqs = append(seqs, s)
		}
		for _, s := range seqs {
			if err := dlq.Replay(ctx, js, s); err != nil {
				return err
			}
			fmt.Printf("replayed dlq seq %d\n", s)
		}
		return nil
	}
	return fmt.Errorf("unknown command %q", args[0])
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
