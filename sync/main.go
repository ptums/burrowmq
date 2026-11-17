// burrow-sync version 1: moves every lane from the device to the cloud, critical first.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	leafURL   = "nats://localhost:4223" // the device's own NATS server
	consumer  = "sync"                  // our bookmark in each lane
	batchSize = 10                      // messages per batch
	minWait   = 1 * time.Second         // first wait after a failure
	maxWait   = 30 * time.Second        // longest wait after many failures
	idleWait  = 500 * time.Millisecond  // pause when every lane is empty
)

type inFlight struct {
	msg    jetstream.Msg
	future jetstream.PubAckFuture
}

// The lanes, most important first. A lane is sent only when every lane above it is empty.
var lanes = []string{"CRITICAL", "BULK"}

func main() {
	// 1. Connect to the device only. The device never talks to the hub directly.
	nc, err := nats.Connect(leafURL, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()

	// Find our bookmark in each lane, in the same order as lanes.
	var consumers []jetstream.Consumer
	for _, lane := range lanes {
		cons, err := js.Consumer(ctx, lane, consumer)
		if err != nil {
			log.Fatalf("find consumer %s/%s: %v", lane, consumer, err)
		}
		consumers = append(consumers, cons)
	}
	log.Printf("burrow-sync v1 started: %v -> hub, batches of %d", lanes, batchSize)

	// How long to wait after a failed batch. It doubles after each failure and resets after a success.
	wait := minWait

	// 2. Loop forever: send 1 batch from the most important lane that has work, then start again from the top.
	for {
		sent := 0
		failed := false
		for i, cons := range consumers {
			sent, failed = sendBatch(ctx, js, cons, lanes[i])
			if sent > 0 || failed {
				break
			}
		}

		if failed {
			time.Sleep(wait)
			wait = min(wait*2, maxWait)
		} else {
			wait = minWait
		}
		if sent == 0 && !failed {
			time.Sleep(idleWait)
		}
	}
}

// sendBatch takes up to batchSize messages from 1 lane and sends them to the hub.
// It returns how many the hub confirmed, and true if something failed.
func sendBatch(ctx context.Context, js jetstream.JetStream, cons jetstream.Consumer, lane string) (int, bool) {
	start := time.Now()

	// NoWait: take what is there now. An empty lane returns at once, so the next lane gets its turn.
	batch, err := cons.FetchNoWait(batchSize)
	if err != nil {
		log.Printf("%s: fetch failed, will retry: %v", lane, err)
		return 0, true
	}

	var flights []inFlight

	failed := false

	for msg := range batch.Messages() {
		if failed {
			_ = msg.Nak()
			continue
		}

		future, err := forward(js, msg, lane)

		if err != nil {
			log.Printf("%s: send failed, will retry: %v", lane, err)
			_ = msg.Nak()
			failed = true
			continue
		}

		flights = append(flights, inFlight{msg, future})
	}

	sent := 0
	for _, f := range flights {
		if failed {
			_ = f.msg.Nak()
			continue
		}
		select {
		case <-f.future.Ok():
			if err := f.msg.Ack(); err != nil {
				log.Printf("%s: ack: %v", lane, err)
			}
			sent++
		case err := <-f.future.Err():
			log.Printf("%s: hub did not confirm, will retry: %v", lane, err)
			_ = f.msg.Nak()
			failed = true
		case <-time.After(10 * time.Second):
			log.Printf("%s: no confirm in 10 s, will retry", lane)
			_ = f.msg.Nak()
			failed = true
		}
	}

	if sent > 0 {
		log.Printf("%s: sent %d", lane, sent)
	}
	if sent > 0 || failed {
		report(js, batchReport{Lane: lane, Sent: sent, Failed: failed, Start: start.UnixMilli(), Ms: time.Since(start).Milliseconds()})
	}
	return sent, failed
}

// batchReport describes 1 batch, for the lab dashboard's batch timeline.
type batchReport struct {
	Lane   string `json:"lane"`
	Sent   int    `json:"sent"`
	Failed bool   `json:"failed"`
	Start  int64  `json:"start"` // when the batch began, in Unix milliseconds
	Ms     int64  `json:"ms"`    // how long the batch took, in milliseconds
}

// report sends 1 batch report as a plain NATS message on the device. Nothing stores it:
// if the dashboard is not listening, the message is dropped, and burrow-sync does not wait.
func report(js jetstream.JetStream, r batchReport) {
	body, err := json.Marshal(r)
	if err != nil {
		return
	}
	_ = js.Conn().Publish("burrow.sync.batch", body)
}

// forward publishes 1 device message to the hub's INGEST stream and sends and does not wait
func forward(js jetstream.JetStream, msg jetstream.Msg, lane string) (jetstream.PubAckFuture, error) {
	meta, err := msg.Metadata()
	if err != nil {
		return nil, err
	}

	// site.device01.bulk.logs.app -> ingest.device01.bulk.logs.app
	subject := "ingest." + strings.TrimPrefix(msg.Subject(), "site.")

	// The same message always gets the same ID, so if we send it twice the hub keeps 1 copy.
	id := msg.Headers().Get(jetstream.MsgIDHeader)
	if id == "" {
		id = fmt.Sprintf("%s-%d", lane, meta.Sequence.Stream)
	}

	out := &nats.Msg{Subject: subject, Data: msg.Data(), Header: nats.Header{}}
	for key, values := range msg.Headers() {
		if strings.HasPrefix(key, "Nats-") {
			continue
		}
		out.Header[key] = values
	}
	out.Header.Set("Burrow-Sent-At", meta.Timestamp.UTC().Format(time.RFC3339Nano)) // the device's original time

	return js.PublishMsgAsync(out, jetstream.WithMsgID(id))
}
