package ncsp

// TODO: error handling: pass down error messages, define
// custom errors (done)
// TODO: logging, logging levels, log for errors (done)
// TODO: configuration (done)
// TODO: reduce verbosity (done)
// TODO: oop/org (done)
// TODO: machines configuration (done)
// TODO: port selection (done)
// TODO: ack
// TODO: Close and clean: Sender does not keep state in etcd; Receiver must clean up after itself
// TODO: Sender should be able to check how many receiversa ure up
// TODO: Shutdown
// TODO: crashes
// TODO: Multiple senders: bus
// TODO: Multiple receivers: broadcast
// TODO: better unit tests
// TODO: add assertions in unit tests
// TODO: TCPAddr
// TODO: Local address
// TODO: file organization
// TODO: buffered channels

import (
	"bytes"
	"github.com/coreos/go-etcd/etcd"
	"net"
	"strconv"
)

/***************************************/

type ChannelIntf interface {
	/* *** Build ***
	 */
	Build(name string, opts *Options) error
	/* *** Close ***
	 */
	// Close() error
}

type SenderChannelIntf interface {
	ChannelIntf
	/* *** Send ***
	 */
	Send(message *bytes.Buffer) error
}

type ReceiverChannelIntf interface {
	ChannelIntf
	/* *** Receive ***
	 */
	Receive() (error, *bytes.Buffer)
}

/***************************************/

type SenderChannel struct {
	Receivers   []string
	UpdatesChan chan *etcd.Response
}

func NewSenderChannel() *SenderChannel {
	var ch SenderChannel
	ch.Receivers = make([]string, 0)
	ch.UpdatesChan = make(chan *etcd.Response)
	return &ch
}

func (ch *SenderChannel) Print() {
	Log.Infoln("Receivers: ", ch.Receivers)
}

func (ch *SenderChannel) Build(name string, opts *Options) error {
	// check if this channel exist already
	// if it exists, fetch receiver(s) info and store locally
	// wait for updates (possibly new receivers) - watch changes -
	// possibly fetch new receivers

	Log.Infoln("Creating SenderChannel: ", name, "options: ", opts)

	option, err := Config.GetOption("etcd.machines")
	ErrCheckFatal(err, "Configuration error")
	machines := ToEtcdMachinesList(option.([]interface{}))
	c := etcd.NewClient(machines)
	response, err := c.Get("/ncsp", true, true)
	if err != nil {
		Log.Errorln("etcd get failed")
		return err
	}
	index := response.EtcdIndex
	response, err = c.Get("/ncsp/"+name+"/receivers", true, true)
	if err != nil {
		Log.Warnln("channel not created yet")
		if EtcdErrorCode(err) != 100 {
			Log.Errorln("etcd get failed")
			return err
		}
	} else {
		for i := range response.Node.Nodes {
			Log.Debugln("Sender is adding a receiver to its list: ", response.Node.Nodes[i].Value)
			ch.Receivers = append(ch.Receivers, response.Node.Nodes[i].Value)
		}
	}
	go func() {
		updates := make(chan *etcd.Response)
		go func() {
			// TODO: what if multiple changes?
			_, err := c.Watch("/ncsp/"+name+"/receivers", index, true, updates, nil)
			ErrCheckFatal(err, "Etcd Watch error")
		}()
		for {
			resp := <-updates
			index = resp.EtcdIndex
			Log.Debugln("Sender is adding a receiver to its list: ", resp.Node.Value)
			ch.Receivers = append(ch.Receivers, resp.Node.Value)
		}
	}()
	return nil
}

/***************************************/

type receiverType struct {
	buf  *bytes.Buffer
	conn net.Conn // FIXME: should it be a pointer?
}

type ReceiverChannel struct {
	Address string
	// response channel
	receiverChan chan receiverType
}

func NewReceiverChannel() *ReceiverChannel {
	var ch ReceiverChannel
	ch.receiverChan = make(chan receiverType)
	return &ch
}

func (ch *ReceiverChannel) Print() {
}

func (ch *ReceiverChannel) Build(name string, opts *Options) error {
	// update configuration
	// start a sever and wait for messages (goroutine that delives to
	// a channel)
	Log.Infoln("Creating ReceiverChannel: ", name, "options: ", opts)
	option, err := Config.GetOption("etcd.machines")
	ErrCheckFatal(err, "Configuration error")
	machines := ToEtcdMachinesList(option.([]interface{}))
	c := etcd.NewClient(machines)
	address := "localhost:" + strconv.FormatUint(uint64(<-Config.Port), 10)
	// FIXME:what if duplicate?
	Log.Debugln("receiver updating address in etcd: ", address)
	_, err = c.CreateInOrder("/ncsp/"+name+"/receivers", address, 0)
	if err != nil {
		Log.Errorln("etcd CreateInOrder failed")
		return err
	}

	// Start server
	ready := make(chan bool)
	go func() {
		// TODO: are we really sure that server is ready?
		ln, err := net.Listen("tcp", address)
		ErrCheckFatal(err, "Listen error")
		ready <- true
		for {
			conn, err := ln.Accept()
			ErrCheckFatal(err, "Accept error")
			buf := new(bytes.Buffer)
			err = ReceiveMessage(conn, buf)
			ErrCheckFatal(err, "ReceiveMessage failed")
			ch.receiverChan <- receiverType{buf, conn}
		}
	}()
	<-ready
	Log.Debugln("server (receiver) is ready")

	return nil
}

// func (ch *ReceiverChannel) Close() error {
// 	return nil
// }

func (ch *SenderChannel) send(addr string, message *bytes.Buffer) error {
	if len(ch.Receivers) > 1 {
		Log.Fatal("Supporting only one receiver per channel for now")
	}
	address := ch.Receivers[0]
	conn, err := net.Dial("tcp", address)
	defer conn.Close()
	if err != nil {
		Log.Errorln("Dial error")
		return err
	}
	err = SendMessage(conn, message)
	if err != nil {
		Log.Errorln("SendMessage failed")
		return err
	}
	buf := new(bytes.Buffer)
	err = ReceiveMessage(conn, buf)
	if err != nil {
		Log.Errorln("ReceiveMessage failed")
		return err
	}
	return nil
}

func (ch *SenderChannel) Send(message *bytes.Buffer) error {
	if len(ch.Receivers) == 0 {
		Log.Errorln("no receivers")
		return NewNcspError("no receivers")
	}
	for i := range ch.Receivers {
		err := ch.send(ch.Receivers[i], message)
		if err != nil {
			Log.Errorln("send failed")
			return err
		}
	}
	return nil
}

func (ch *ReceiverChannel) Receive() (*bytes.Buffer, error) {
	tmp := <-ch.receiverChan
	response := tmp.buf
	conn := tmp.conn
	Log.Debugln("got message: ", response, "connection: ", conn)
	// FIXME: replace string message 'ack'
	ack := bytes.NewBufferString("ack")
	err := SendMessage(conn, ack)
	if err != nil {
		Log.Errorln("SendMessage failed")
		return nil, err
	}
	conn.Close()
	return response, err
}
