package ncsp

// TODO: Close and clean: Sender does not keep state in etcd; Receiver must clean up after itself
// TODO: Sender should be able to check how many receiversa ure up
// TODO: Shutdown
// TODO: crashes
// TODO: Multiple senders: bus
// TODO: Multiple receivers: broadcast
// TODO: TCPAddr
// TODO: Local address
// TODO: file organization
// TODO: buffered channels: will it work with new receiver?
// TODO: Method to check if a channel is closed
// TODO: test generates its own config file
// TODO: add receiver timeout
// TODO: look more at the watch loop... may be a race

import (
	"bytes"
	"github.com/coreos/go-etcd/etcd"
	"net"
	"reflect"
	"strconv"
	"time"
)

/***************************************/

type ChannelIntf interface {
	/* *** Build ***
	 */
	Build(name string, opts *Options) error
	/* *** Close ***
	 */
	Close() error
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
	broadcaster *Broadcaster
}

func NewSenderChannel() *SenderChannel {
	var ch SenderChannel
	ch.Receivers = make([]string, 0)
	ch.UpdatesChan = make(chan *etcd.Response)
	ch.broadcaster = NewBroadcaster(1)
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

	Log.Debugln("Creating SenderChannel: ", name, "options: ", opts)

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
	// Handle updates: please verify I always get all updates
	// TODO: how do watch handle past updates? looks like there might be a race here
	// Look at how index works
	go func() {
		updates := make(chan *etcd.Response)
		go func() {
			// TODO: what if multiple changes?
			stop := ch.broadcaster.Listen()
			_, err := c.Watch("/ncsp/"+name+"/receivers", index, true, updates, stop)
			Log.Debugln("Etcd Watch error:", err)
		}()
		updatesStop := ch.broadcaster.Listen()
		for {
			// FIXME: triggered on every updates!.. what happen on a delete?
			// FIXME: maybe just go and re-read
			// getting multiple updates, why?
			select {
			case resp := <-updates:
				if resp != nil {
					Log.Debugln("->", reflect.TypeOf(resp))
					Log.Debugln("->", resp)
					Log.Debugln("->", resp.Node)
					switch resp.Action {
					case "set":
						Log.Debugln("Sender is adding a receiver to its list: ", resp.Node.Value, len(ch.Receivers))
						ch.Receivers = append(ch.Receivers, resp.Node.Value)
						if len(ch.Receivers) > 1 {
							Log.Fatal("supporting only 1 receiver per channel, receivers:", ch.Receivers, len(ch.Receivers))
						}
					case "delete":
						Log.Debugln("delete Not implemented")
						// Log.Debugln("Sender removing a receiver to its list: ", resp.Node.Value)
						// for i, e := range ch.Receivers {
						// 	if e ==
						// }
						// ch.Receivers = append(ch.Receivers[:i], ch.Receivers[i+1:]...)
					}
				}
			case <-updatesStop:
				Log.Debugln("Exiting update loop")
				break
			}
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
	Name    string
	Address string
	// response channel
	receiverChan chan receiverType
	Listener     net.Listener
}

func NewReceiverChannel() *ReceiverChannel {
	var ch ReceiverChannel
	ch.receiverChan = make(chan receiverType)
	return &ch
}

func (ch *ReceiverChannel) Print() {
}

func (ch *ReceiverChannel) Build(name string, opts *Options) error {
	ch.Name = name
	// update configuration
	Log.Debugln("Creating ReceiverChannel: ", name, "options: ", opts)
	option, err := Config.GetOption("etcd.machines")
	ErrCheckFatal(err, "Configuration error")
	machines := ToEtcdMachinesList(option.([]interface{}))
	c := etcd.NewClient(machines)
	// FIXME: localhost
	address := "localhost:" + strconv.FormatUint(uint64(<-Config.Port), 10)
	Log.Debug("receiver address:", address)
	ch.Address = address
	_, err = c.Set("/ncsp/"+name+"/receivers/"+address, address, 0)
	if err != nil {
		Log.Errorln("etcd Set has")
		return err
	}

	ch.Listener, err = net.Listen("tcp", ch.Address)
	if err != nil {
		Log.Errorln(err, "Listen error")
		return err
	}

	return nil
}

// FIXME: Should this be blocking?! Is listener up?
func (ch *ReceiverChannel) Close() error {
	// FIXME: multiple receivers
	// FIXME: localhost
	option, err := Config.GetOption("etcd.machines")
	ErrCheckFatal(err, "Configuration error")
	machines := ToEtcdMachinesList(option.([]interface{}))
	c := etcd.NewClient(machines)
	_, err = c.Delete("/ncsp/"+ch.Name, true)
	if err != nil {
		Log.Errorln("Delete error", err)
		return err
	}
	Log.Debugln("Closed receiver channel")
	return nil
}

func (ch *SenderChannel) Close() error {
	// FIXME: multiple receivers
	// FIXME: localhost
	ch.Receivers = nil

	Log.Debugln("brodcasting stop signal")
	for err := ch.broadcaster.Write(true, 2); err != nil; {
		Log.Debugln("wait")
		time.Sleep(100 * time.Millisecond)
		err = ch.broadcaster.Write(true, 2)
	}
	Log.Debugln("Closed sender channel")
	return nil
}

func (ch *SenderChannel) send(addr string, message *bytes.Buffer) error {
	conn, err := net.Dial("tcp", addr)
	defer conn.Close() // Race here??
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
		Log.Debugln("no receivers")
		return NewNcspError("no receivers")
	}
	for i := range ch.Receivers {
		Log.Debugln("To: ", ch.Receivers[i]) // FIXME: receiver is not valid!!
		err := ch.send(ch.Receivers[i], message)
		if err != nil {
			Log.Errorln("send failed")
			return err
		}
	}
	return nil
}

func (ch *ReceiverChannel) Receive() (*bytes.Buffer, error) {
	Log.Debugln("receive")
	conn, err := ch.Listener.Accept()
	response := new(bytes.Buffer)
	err = ReceiveMessage(conn, response)
	if err != nil {
		Log.Errorln(err, "Receive error")
		return nil, err
	}
	Log.Debugln("got message, ", response.Bytes(), "connection: ", conn)
	err = SendZero(conn)
	if err != nil {
		Log.Errorln("SendMessage failed")
		return nil, err
	}
	conn.Close()
	return response, err
}
