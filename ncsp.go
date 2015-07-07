package ncsp

// TODO: error handling: pass down error messages, define
// custom errors (done)
// TODO: logging, logging levels, log for errors (done)
// TODO: configuration (done)
// TODO: reduce verbosity (done)
// TODO: oop/org (done)
// TODO: machines configuration (done)
// TODO: port selection (done)
// TODO: TCPAddr
// TODO: Local address
// TODO: better unit tests
// TODO: add assertions in unit tests
// TODO: file organization
// TODO: buffer
// TODO: ack

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
	Log.Debugln("first attempt")
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
	Log.Debugln("updating address: ", address)
	response, err := c.CreateInOrder("/ncsp/"+name+"/receivers", address, 0)
	if err != nil {
		Log.Errorln("etcd CreateInOrder")
		return err
	}
	Log.Debugln("Receiver CreateInOrder done, index:", response.EtcdIndex)

	// Start server
	ready := make(chan bool)
	go func() {
		// TODO: make sure the server is ready
		Log.Debugln("server goes here")
		ln, err := net.Listen("tcp", ":33333")
		ErrCheckFatal(err, "Listen error")
		ready <- true
		for {
			conn, err := ln.Accept() // TODO: how to close this?
			ErrCheckFatal(err, "Accept error")
			buf := new(bytes.Buffer)
			err = ReceiveMessage(conn, buf)
			ErrCheckFatal(err, "ReceiveMessage failed")
			Log.Debugln("Receiver writing msg to channel")
			ch.receiverChan <- receiverType{buf, conn}
			Log.Debugln("...sent")
		}
	}()
	Log.Debugln("waiting for server to be ready")
	<-ready
	Log.Debugln("server is ready")

	return nil
}

// func (ch *ReceiverChannel) Close() error {
// 	return nil
// }

func (ch *SenderChannel) send(addr string, message *bytes.Buffer) error {
	conn, err := net.Dial("tcp", ":33333") // TODO: use Receivers
	defer conn.Close()                     // TODO: close...
	Log.Debugln("Conn: ", conn)
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

// TODO: byte buffer pointer
func (ch *ReceiverChannel) Receive() (*bytes.Buffer, error) {
	Log.Debugln("blocking for message: ")
	tmp := <-ch.receiverChan
	Log.Debugln("...received")
	response := tmp.buf
	conn := tmp.conn
	Log.Debugln("got message: ", response, conn)
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
