package ncsp

// TODO: error handling: pass down error messages, define
// custom errors
// TODO: logging, logging levels, log for errors
// TODO: reduce verbosity
// TODO: oop
// TODO: TCPAddr
// TODO: machines configuration
// TODO: better unit tests
// TODO: file organization
// TODO: configuration
// TODO: buffer

import (
	"bytes"
	"github.com/coreos/go-etcd/etcd"
	"log"
	"net"
)

type ChannelIntf interface {
	/* *** Build ***
	 */
	Build(name string, buffer uint32) error
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

func (ch *ReceiverChannel) Print() {
}

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

func (ch *SenderChannel) Print() {
	log.Println("Receivers: ", ch.Receivers)
}

func (ch *SenderChannel) Build(name string, buffer uint32) error {
	// check if this channel exist already
	// if it exists, fetch receiver(s) info and store locally
	// wait for updates (possibly new receivers) - watch changes -
	// possibly fetch new receivers

	//FIXME: each channel may use the same client
	machines := []string{"http://127.0.0.1:2379"}
	c := etcd.NewClient(machines)
	log.Println("first attempt")
	response, err := c.Get("/ncsp", true, true)
	if err != nil {
		log.Println("etcd get failed")
		return err
	}
	index := response.EtcdIndex
	response, err = c.Get("/ncsp/"+name+"/receivers", true, true)
	if err != nil {
		log.Println("Warning: channel not created yet")
		if EtcdErrorCode(err) != 100 {
			log.Println("etcd get failed")
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

func (ch *ReceiverChannel) Build(name string, buffer uint32) error {
	// update configuration
	// start a sever and wait for messages (goroutine that delives to
	// a channel)
	machines := []string{"http://127.0.0.1:2379"}
	c := etcd.NewClient(machines)
	address := "localhost:33333"

	// FIXME:what if duplicate?
	log.Println("updating address")
	response, err := c.CreateInOrder("/ncsp/"+name+"/receivers", address, 0)
	if err != nil {
		log.Println("etcd CreateInOrder")
		return err
	}
	log.Println("Receiver CreateInOrder done, index:", response.EtcdIndex)

	// Start server
	ready := make(chan bool)
	go func() {
		// TODO: make sure the server is ready
		log.Println("server goes here")
		ln, err := net.Listen("tcp", ":33333")
		ErrCheckFatal(err, "Listen error")
		ready <- true
		for {
			conn, err := ln.Accept() // TODO: how to close this?
			ErrCheckFatal(err, "Accept error")
			buf := new(bytes.Buffer)
			err = ReceiveMessage(conn, buf)
			ErrCheckFatal(err, "ReceiveMessage failed")
			log.Println("Receiver writing msg to channel")
			ch.receiverChan <- receiverType{buf, conn}
			log.Println("...sent")
		}
	}()
	log.Println("waiting for server to be ready")
	<-ready
	log.Println("server is ready")

	return nil
}

// func (ch *ReceiverChannel) Close() error {
// 	return nil
// }

func (ch *SenderChannel) send(addr string, message *bytes.Buffer) error {
	conn, err := net.Dial("tcp", ":33333") // TODO: use Receivers
	defer conn.Close()                     // TODO: close...
	log.Println("Conn: ", conn)
	if err != nil {
		log.Println("Dial error")
		return err
	}
	err = SendMessage(conn, message)
	if err != nil {
		log.Println("SendMessage failed")
		return err
	}
	buf := new(bytes.Buffer)
	err = ReceiveMessage(conn, buf)
	if err != nil {
		log.Println("ReceiveMessage failed")
		return err
	}
	return nil
}

func (ch *SenderChannel) Send(message *bytes.Buffer) error {
	if len(ch.Receivers) == 0 {
		log.Println("no receivers")
		return NewNcspError("no receivers")
	}
	for i := range ch.Receivers {
		err := ch.send(ch.Receivers[i], message)
		if err != nil {
			log.Println("send failed")
			return err
		}
	}
	return nil
}

// TODO: byte buffer pointer
func (ch *ReceiverChannel) Receive() (*bytes.Buffer, error) {
	log.Println("blocking for message: ")
	tmp := <-ch.receiverChan
	log.Println("...received")
	response := tmp.buf
	conn := tmp.conn
	log.Println("got message: ", response, conn)
	// ack
	ack := bytes.NewBufferString("ack")
	err := SendMessage(conn, ack)
	if err != nil {
		log.Println("SendMessage failed")
		return nil, err
	}
	conn.Close() // FIXME: check this
	return response, err
}
