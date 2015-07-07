package ncsp

import (
	// "errors"
	"bytes"
	"github.com/coreos/go-etcd/etcd"
	"log"
	"reflect"
	"testing"
	"time"
)

func sender_process(done chan bool) {
	Log.Debugln("Sender process")
	ch := NewSenderChannel()
	opts := NewOptions()
	// TODO: make 1 call!
	opts.AddOption("buffer", reflect.Uint32)
	opts.SetOption("buffer", 0)
	err := ch.Build("channel0", opts)
	ErrCheckFatal(err, "Cannot build sender channel")
	msg := bytes.NewBufferString("ciao")
	err = ch.Send(msg)
	for err != nil {
		Log.Debugln("Receiver not ready yet")
		time.Sleep(time.Second)
		err = ch.Send(msg)
	}
	ErrCheckFatal(err, "Send failed")
	Log.Debugln("Sending")
	msg = bytes.NewBufferString("ciao again")
	err = ch.Send(msg)
	Log.Debugln("... Send done")
	done <- true
}

func receiver_process(done chan bool) {
	Log.Debugln("Receiver process")
	ch := NewReceiverChannel()
	// this start a server in the background
	// each send/receive works on a
	// different tcp connection
	opts := NewOptions()
	opts.AddOption("buffer", reflect.Uint32)
	opts.SetOption("buffer", 0)
	err := ch.Build("channel0", opts)
	ErrCheckFatal(err, "Cannot build receiver channel")
	Log.Debugln("Receiving")
	resp, err := ch.Receive()
	ErrCheckFatal(err, "Receive failed")
	Log.Debugln("Response: ", resp)
	Log.Debugln("Receiving")
	resp, err = ch.Receive()
	ErrCheckFatal(err, "Receive failed")
	Log.Debugln("Response: ", resp)
	done <- true
}

func prepare() {
	// cleanup
	machines := []string{"http://127.0.0.1:2379"}
	c := etcd.NewClient(machines)
	err := c.SetConsistency(etcd.STRONG_CONSISTENCY)
	ErrCheckFatal(err, "Consistency")
	_, err = c.Get("/ncsp", false, false)
	if err != nil {
		Log.Warnln("Warning: /ncsp not found")
		if EtcdErrorCode(err) != 100 {
			log.Fatal(err, "Get failed")
		}
	} else {
		_, err = c.Delete("/ncsp", true)
		ErrCheckFatal(err, "Cannot delete")
	}
	_, err = c.CreateDir("/ncsp/", 0)
	ErrCheckFatal(err, "Creating root dir")
}

func Test1(t *testing.T) {
	prepare()
	done := make(chan bool)
	go sender_process(done)
	go receiver_process(done)
	<-done
	<-done
}
