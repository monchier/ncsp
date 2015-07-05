package ncsp

import (
	// "errors"
	"bytes"
	"github.com/coreos/go-etcd/etcd"
	"log"
	"testing"
	"time"
)

func sender_process(done chan bool) {
	log.Println("Sender process")
	ch := NewSenderChannel()
	// var ch SenderChannel
	// time.Sleep(time.Second)
	err := ch.Build("channel0", 0)
	ErrCheckFatal(err, "Cannot build sender channel")
	msg := bytes.NewBufferString("ciao")
	err = ch.Send(msg)
	for err != nil {
		log.Println("Receiver not ready yet")
		time.Sleep(time.Second)
		err = ch.Send(msg)
	}
	ErrCheckFatal(err, "Send failed")
	log.Println("Sending")
	msg = bytes.NewBufferString("ciao again")
	err = ch.Send(msg)
	log.Println("... Send donw")
	done <- true
}

func receiver_process(done chan bool) {
	log.Println("Receiver process")
	ch := NewReceiverChannel()
	// this start a server in the background
	// each send/receive works on a
	// different tcp connection
	err := ch.Build("channel0", 0)
	ErrCheckFatal(err, "Cannot build receiver channel")
	log.Println("Receiving")
	resp, err := ch.Receive()
	ErrCheckFatal(err, "Receive failed")
	log.Println("Response: ", resp)
	log.Println("Receiving")
	resp, err = ch.Receive()
	ErrCheckFatal(err, "Receive failed")
	log.Println("Response: ", resp)
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
		log.Println("Warning: /ncsp not found")
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
