package ncsp

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"github.com/coreos/go-etcd/etcd"
	"reflect"
	"testing"
	"time"
)

func sender_1_1_process(done chan bool, messages chan []byte) {
	Log.Debugln("Sender process")
	ch := NewSenderChannel()
	opts := NewOptions()
	// TODO: make 1 call!
	// TODO: maybe Options is an overkill?
	opts.AddOption("buffer", reflect.Uint32)
	opts.SetOption("buffer", 0)
	err := ch.Build("channel0", opts)
	ErrCheckFatal(err, "Cannot build sender channel")

	for j := 0; j < 100; j++ {
		buf := make([]byte, 16)
		_, err = rand.Read(buf)
		ErrCheckFatal(err, "Random number error")
		messages <- buf

		msg := bytes.NewBuffer(buf)
		err = ch.Send(msg)
		for err != nil {
			Log.Debugln("Receiver not ready yet")
			time.Sleep(time.Second)
			Log.Debugln("\tSending")
			err = ch.Send(msg)
			Log.Debugln("\t... Sent")
		}
		ErrCheckFatal(err, "Send failed")
	}
	done <- true
	ch.Close()
}

func receiver_1_1_process(done chan bool, messages chan []byte) {
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

	for j := 0; j < 100; j++ {
		Log.Debugln("\tReceiving")
		resp, err := ch.Receive()
		ErrCheckFatal(err, "Receive failed")
		buf := <-messages
		Log.Debugln("\tReceived: ", resp.Bytes(), " - source: ", buf)
		for i := range resp.Bytes() {
			if resp.Bytes()[i] != buf[i] {
				Log.Fatal("Receivd wrong data - i: ", i, " resp[i]: ", resp.Bytes()[i], "buf[i]: ", buf[i])
			}
		}
	}

	done <- true
	ch.Close()
}

func sender_many_1_process(n int, how_many int, done chan bool, table map[int][sha1.Size]byte) {
	Log.Debugln("Sender process")
	for i := 0; i < how_many; i++ {
		go func(n int, i int, done chan bool, table map[int][sha1.Size]byte) {
			Log.Debugln("sender goroutine", i)
			ch := NewSenderChannel()
			opts := NewOptions()
			opts.AddOption("buffer", reflect.Uint32)
			opts.SetOption("buffer", 0)
			err := ch.Build("channel0", opts)
			ErrCheckFatal(err, "Cannot build sender channel")

			for j := 0; j < n; j++ {
				buf := make([]byte, 16)
				_, err = rand.Read(buf)
				table[i*n+j] = sha1.Sum(buf)
				ErrCheckFatal(err, "Random number error")
				// add a unique ID here
				msg := new(bytes.Buffer)
				_, err = msg.WriteRune(rune(i*n + j))
				ErrCheckFatal(err, "Write buffer error")
				_, err = msg.Write(buf)
				ErrCheckFatal(err, "Write buffer error")
				err = ch.Send(msg)
				for err != nil {
					Log.Debugln("Receiver not ready yet")
					time.Sleep(time.Second)
					Log.Debugln("\tSending")
					err = ch.Send(msg)
					Log.Debugln("\t... Sent")
				}
				ErrCheckFatal(err, "Send failed")
			}
			done <- true
			ch.Close()
		}(n, i, done, table)
	}
}

func receiver_many_1_process(n int, how_many int, done chan bool, table map[int][sha1.Size]byte) {
	Log.Debugln("Receiver process")
	ch := NewReceiverChannel()
	opts := NewOptions()
	opts.AddOption("buffer", reflect.Uint32)
	opts.SetOption("buffer", 0)
	err := ch.Build("channel0", opts)
	ErrCheckFatal(err, "Cannot build receiver channel")
	for j := 0; j < n*how_many; j++ {
		Log.Debugln("\tReceiving ", j)
		resp, err := ch.Receive()
		ErrCheckFatal(err, "Receive failed")
		id, _, err := resp.ReadRune()
		ErrCheckFatal(err, "Failed reading")
		if len(resp.Bytes()) != 16 {
			Log.Fatal("received #", len(resp.Bytes()), "bytes")
		}
		sha := sha1.Sum(resp.Bytes())
		Log.Debugln("\tReceived, id:", id, "bytes: ", resp.Bytes(), "sha", sha)
		for i := range sha {
			if table[int(id)][i] != sha[i] {
				Log.Fatal("Receivd wrong data - id:", int(id), "i: ", i, " sha[i]: ", sha[i], "table[id][i]: ", table[int(id)][i])
			}
		}
	}
	done <- true
	ch.Close()
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

// Test1: Very basic 1 sender - 1 receiver test
func Test1(t *testing.T) {
	prepare()
	done := make(chan bool)
	messages := make(chan []byte, 1)
	go sender_1_1_process(done, messages)
	go receiver_1_1_process(done, messages)
	<-done
	<-done
	shutdown()
}

// Test2: Many sender test
// FIXME: add data check
// FIXME: it seems we keep adding channels to etcd even if a channel is already there
func Test2(t *testing.T) {
	prepare()
	done := make(chan bool)
	how_many := int(100)
	n := int(100)
	table := make(map[int][sha1.Size]byte)
	go sender_many_1_process(n, how_many, done, table)
	go receiver_many_1_process(n, how_many, done, table)
	for j := 0; j < how_many+1; j++ {
		<-done
		Log.Debugln("---> Done", j)
	}
	Log.Debugln("---> Shutting down")
	shutdown()
}
