package ncsp

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"github.com/coreos/go-etcd/etcd"
	"net/http"
	_ "net/http/pprof"
	"reflect"
	"strconv"
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

func sender_many_1_process(n int, how_many int, done chan bool, objects [][]byte) {
	Log.Debugln("Sender process")
	for i := 0; i < how_many; i++ {
		go func(n int, i int, done chan bool, objects [][]byte) {
			Log.Debugln("sender goroutine", i)
			ch := NewSenderChannel()
			opts := NewOptions()
			opts.AddOption("buffer", reflect.Uint32)
			opts.SetOption("buffer", 0)
			err := ch.Build("channel0", opts)
			ErrCheckFatal(err, "Cannot build sender channel")

			for j := 0; j < n; j++ {
				// add a unique ID here
				msg := new(bytes.Buffer)
				_, err = msg.WriteRune(rune(i*n + j))
				ErrCheckFatal(err, "Write buffer error")
				_, err = msg.Write(objects[i*n+j])
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
		}(n, i, done, objects)
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

func sender_many_many_process(n int, how_many int, done chan bool, objects [][]byte) {
	Log.Debugln("Sender process")
	for i := 0; i < how_many; i++ {
		go func(n int, i int, done chan bool, objects [][]byte) {
			Log.Debugln("sender goroutine", i)
			ch := NewSenderChannel()
			opts := NewOptions()
			opts.AddOption("buffer", reflect.Uint32)
			opts.SetOption("buffer", 0)
			err := ch.Build("channel"+strconv.Itoa(i), opts)
			ErrCheckFatal(err, "Cannot build sender channel")

			for j := 0; j < n; j++ {
				// add a unique ID here
				msg := new(bytes.Buffer)
				_, err = msg.WriteRune(rune(i*n + j))
				ErrCheckFatal(err, "Write buffer error")
				_, err = msg.Write(objects[i*n+j])
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
		}(n, i, done, objects)
	}
}
func receiver_many_many_process(n int, how_many int, done chan bool, table map[int][sha1.Size]byte) {
	Log.Debugln("Receiver process")
	chs := make([]*ReceiverChannel, how_many)
	for i := 0; i < how_many; i++ {
		chs[i] = NewReceiverChannel()
		opts := NewOptions()
		opts.AddOption("buffer", reflect.Uint32)
		opts.SetOption("buffer", 0)
		err := chs[i].Build("channel"+strconv.Itoa(i), opts)
		ErrCheckFatal(err, "Cannot build receiver channel: "+strconv.Itoa(i))
	}
	for i := 0; i < how_many; i++ {
		go func(ch *ReceiverChannel, done chan bool, table map[int][sha1.Size]byte) {
			for j := 0; j < n; j++ {
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
				for k := range sha {
					if table[int(id)][k] != sha[k] {
						Log.Fatal("Receivd wrong data - id:", int(id), "k: ", k, " sha[i]: ", sha[k], "table[id][k]: ", table[int(id)][k])
					}
				}
			}
			done <- true
			ch.Close()
		}(chs[i], done, table)
	}
}

func sender_many_many_process2(chs []*SenderChannel, n int, how_many int, done chan bool, objects [][]byte) {
	Log.Debugln("Sender process")
	for i := 0; i < how_many; i++ {
		go func(n int, i int, done chan bool, objects [][]byte) {
			Log.Debugln("sender goroutine", i)
			for j := 0; j < n; j++ {
				// add a unique ID here
				msg := new(bytes.Buffer)
				_, err := msg.WriteRune(rune(i*n + j))
				ErrCheckFatal(err, "Write buffer error")
				_, err = msg.Write(objects[i*n+j])
				ErrCheckFatal(err, "Write buffer error")
				if chs[i] == nil {
					Log.Fatal("no channel")
				}
				err = chs[i].Send(msg)
				ErrCheckFatal(err, "I'm not expeting this in this test")
			}
			done <- true
			chs[i].Close()
		}(n, i, done, objects)
	}
}
func receiver_many_many_process2(chs []*ReceiverChannel, n int, how_many int, done chan bool, table map[int][sha1.Size]byte) {
	Log.Debugln("Receiver process")
	for i := 0; i < how_many; i++ {
		go func(ch *ReceiverChannel, done chan bool, table map[int][sha1.Size]byte) {
			for j := 0; j < n; j++ {
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
				for k := range sha {
					if table[int(id)][k] != sha[k] {
						Log.Fatal("Receivd wrong data - id:", int(id), "k: ", k, " sha[i]: ", sha[k], "table[id][k]: ", table[int(id)][k])
					}
				}
			}
			done <- true
			ch.Close()
		}(chs[i], done, table)
	}
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
	go func() {
		Log.Infoln(http.ListenAndServe("localhost:6060", nil))
	}()
	prepare()
	done := make(chan bool)
	how_many := int(10)
	n := int(100)

	// generate files
	objects := make([][]byte, how_many*n)
	table := make(map[int][sha1.Size]byte)
	for i := 0; i < how_many*n; i++ {
		objects[i] = make([]byte, 16)
		_, err := rand.Read(objects[i])
		ErrCheckFatal(err, "Random number error")
		table[i] = sha1.Sum(objects[i])
	}
	start := time.Now()
	go sender_many_1_process(n, how_many, done, objects)
	go receiver_many_1_process(n, how_many, done, table)
	for j := 0; j < how_many+1; j++ {
		<-done
		Log.Debugln("---> Done", j)
	}
	end := time.Now()
	elapsed := end.Sub(start)
	Log.Infoln("Time elapsed [ms]:", float64(elapsed)/1e6, "Req/s:", float64(how_many)*float64(n)/float64(elapsed)*1e9)
	Log.Debugln("---> Shutting down")
	shutdown()
}

func Test3(t *testing.T) {
	prepare()
	done := make(chan bool)
	how_many := int(10)
	n := int(1)

	// generate files
	objects := make([][]byte, how_many*n)
	table := make(map[int][sha1.Size]byte)
	for i := 0; i < how_many*n; i++ {
		objects[i] = make([]byte, 16)
		_, err := rand.Read(objects[i])
		ErrCheckFatal(err, "Random number error")
		table[i] = sha1.Sum(objects[i])
	}
	Log.Infoln("Starting test")
	start := time.Now()
	go sender_many_many_process(n, how_many, done, objects)
	go receiver_many_many_process(n, how_many, done, table)
	for j := 0; j < 2*how_many; j++ {
		<-done
		Log.Debugln("---> Done", j)
	}
	end := time.Now()
	Log.Infoln("Ending test")
	elapsed := end.Sub(start)
	Log.Infoln("Time elapsed [ms]:", float64(elapsed)/1e6, "Req/s:", float64(how_many)*float64(n)/float64(elapsed)*1e9)
	Log.Debugln("---> Shutting down")

	shutdown()
}

func Test4(t *testing.T) {
	prepare()
	done := make(chan bool)
	how_many := int(10)
	n := int(10)

	// generate files
	objects := make([][]byte, how_many*n)
	table := make(map[int][sha1.Size]byte)
	for i := 0; i < how_many*n; i++ {
		objects[i] = make([]byte, 16)
		_, err := rand.Read(objects[i])
		ErrCheckFatal(err, "Random number error")
		table[i] = sha1.Sum(objects[i])
	}

	// build sender channels
	senders := make([]*SenderChannel, how_many)
	for i := range senders {
		senders[i] = NewSenderChannel()
		opts := NewOptions()
		opts.AddOption("buffer", reflect.Uint32)
		opts.SetOption("buffer", 0)
		err := senders[i].Build("channel"+strconv.Itoa(i), opts)
		ErrCheckFatal(err, "Cannot build sender channel")
	}
	// build receiver channels
	receivers := make([]*ReceiverChannel, how_many)
	for i := range receivers {
		receivers[i] = NewReceiverChannel()
		opts := NewOptions()
		opts.AddOption("buffer", reflect.Uint32)
		opts.SetOption("buffer", 0)
		err := receivers[i].Build("channel"+strconv.Itoa(i), opts)
		ErrCheckFatal(err, "Cannot build receiver channel")
	}

	start := time.Now()
	go sender_many_many_process2(senders, n, how_many, done, objects)
	go receiver_many_many_process2(receivers, n, how_many, done, table)
	for j := 0; j < 2*how_many; j++ {
		<-done
		Log.Debugln("---> Done", j)
	}
	end := time.Now()
	elapsed := end.Sub(start)
	Log.Infoln("Time elapsed [ms]:", float64(elapsed)/1e6, "Req/s:", float64(how_many)*float64(n)/float64(elapsed)*1e9)
	Log.Debugln("---> Shutting down")

	shutdown()
}
