package ncsp

import (
	"bytes"
	"github.com/coreos/go-etcd/etcd"
	"reflect"
	"testing"
	"time"
)

func sender_1_1_process(done chan bool) {
	Log.Debugln("Sender process")
	ch := NewSenderChannel()
	opts := NewOptions()
	// TODO: make 1 call!
	// TODO: maybe Options is an overkill?
	opts.AddOption("buffer", reflect.Uint32)
	opts.SetOption("buffer", 0)
	err := ch.Build("channel0", opts)
	ErrCheckFatal(err, "Cannot build sender channel")
	msg := bytes.NewBufferString("ciao")
	err = ch.Send(msg)
	for err != nil {
		Log.Debugln("Receiver not ready yet")
		time.Sleep(time.Second)
		Log.Debugln("\tSending")
		err = ch.Send(msg)
		Log.Debugln("\t... Sent")
	}
	ErrCheckFatal(err, "Send failed")
	Log.Debugln("\tSending")
	msg = bytes.NewBufferString("ciao again")
	err = ch.Send(msg)
	Log.Debugln("\t... Sent")
	done <- true
}

func receiver_1_1_process(done chan bool) {
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
	Log.Debugln("\tReceiving")
	resp, err := ch.Receive()
	ErrCheckFatal(err, "Receive failed")
	Log.Debugln("\tReceived: ", resp)
	Log.Debugln("\tReceiving")
	resp, err = ch.Receive()
	ErrCheckFatal(err, "Receive failed")
	Log.Debugln("\tReceived: ", resp)
	done <- true
}

func prepare() {
	// cleanup
	Config.Init("conf.json")
	option, err := Config.GetOption("etcd.machines")
	ErrCheckFatal(err, "Configuration error")
	machines := ToEtcdMachinesList(option.([]interface{}))
	c := etcd.NewClient(machines)
	err = c.SetConsistency(etcd.STRONG_CONSISTENCY) // TODO: is this valid for other clients as well?
	ErrCheckFatal(err, "Consistency")
	_, err = c.Get("/ncsp", false, false)
	if err != nil {
		Log.Warnln("Warning: /ncsp not found")
		if EtcdErrorCode(err) != 100 {
			Log.Fatal(err, "Get failed")
		}
	} else {
		_, err = c.Delete("/ncsp", true)
		ErrCheckFatal(err, "Cannot delete")
	}
	_, err = c.CreateDir("/ncsp/", 0)
	ErrCheckFatal(err, "Creating root dir")
}

func shutdown() {
	Log.Debugln("Start shutdown (ie etcd cleanup)")
	option, err := Config.GetOption("etcd.machines")
	ErrCheckFatal(err, "Configuration error")
	machines := ToEtcdMachinesList(option.([]interface{}))
	c := etcd.NewClient(machines)
	_, err = c.Get("/ncsp", false, false)
	if err != nil {
		Log.Warnln("Warning: /ncsp not found")
		if EtcdErrorCode(err) != 100 {
			Log.Fatal(err, "Get failed")
		}
	} else {
		_, err = c.Delete("/ncsp", true)
		ErrCheckFatal(err, "Cannot delete")
	}
	Log.Infoln("Shutdown done and etcd has been cleaned up")
}

// Very basic 1 sender - 1 receiver test
func Test1(t *testing.T) {
	prepare()
	done := make(chan bool)
	go sender_1_1_process(done)
	go receiver_1_1_process(done)
	<-done
	<-done
	shutdown()
}
