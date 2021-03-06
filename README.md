# ncsp

Implementation of CSP (Communicating Sequential Processes) over the network. Network-version of golang channels.

Only basic functionality is there. Work in progress... 

NCSP uses etcd for configuration. etcd needs to be running before any use of NCSP.

Run go test for a simple sender/receiver example

## example usage
Note: the code below is intentianlly high level and abstracts from details. It will not work if just cut and paste. Refer to [ncsp_test.go](http://github.com/monchier/ncsp/blob/master/ncsp_test.go) for full working code.


#### Receiver:
```
	ch := NewReceiverChannel()                // Create a new empty receiver channel
	opts := NewOptions()                      // Initialize and set options
	opts.AddOption("buffer", reflect.Uint32)  // Initialize and set options
	opts.SetOption("buffer", 0)               // Initialize and set options
	
	err := ch.Build("channel0", opts)         // Build channel with a given name and options
	                                          // This step instantiates a receiver-side server 
	                                          // for incoming requests
	                                          
	resp, err := ch.Receive()                 // Block and receive. When a message is received 
	                                          // ack back and return received message
```

#### Sender:
```
	ch := NewSenderChannel()                  // Create a new empty receiver channel
	opts := NewOptions()                      // Initialize and set options
	opts.AddOption("buffer", reflect.Uint32)  // Initialize and set options
	opts.SetOption("buffer", 0)               // Initialize and set options
	
	err := ch.Build("channel0", opts)         // Build channel with a given name and options
						  // The channel checks for existing receivers and watch 
						  // for new receivers that may join later 
	
	buf := make([]byte, 16)                   // Initialize byte buffer 
	_, err = rand.Read(buf)                   // Fill the buffer with random bits
	msg := bytes.NewBuffer(buf)               // Create a bytes.Buffer
	err = ch.Send(msg)                        // Send message. It will block until the reception 
	                                          // has been ackwnoleged. It may fail if the receiver
	                                          // is not ready yet
```





