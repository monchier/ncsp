package ncsp

import (
	"reflect"
	"testing"
	"time"
)

// TODO: validators
func TestOptions(t *testing.T) {
	Log.Debugln("Testing options.go")
	opts := NewOptions()
	opts.AddOption("test", reflect.Bool)
	err := opts.SetOption("test", 123)
	Log.Debugln("Error: ", err, " type: ", reflect.TypeOf(err))
	if err == nil {
		Log.Fatal("TestOption: It should not be possible to set a",
			"boolean option with an integer")
	}
	err = opts.SetOption("test", true)
	if err != nil {
		Log.Fatal("TestOption: Failed to set option", err)
	}
	value := opts.GetOption("test")
	Log.Debugln("value of the option is ", value)
	if reflect.TypeOf(value).Kind() != reflect.Bool {
		Log.Fatal("TestOption: Returned object with wrong type: ", reflect.TypeOf(value).Kind())
	}
	opts.AddOption("list_option", reflect.Slice)
	aSlice := []string{"value1", "value2"}
	err = opts.SetOption("list_option", aSlice)
	if err != nil {
		Log.Fatal("TestOption: Failed to set option", err)
	}
	list_value := opts.GetOption("list_option")
	Log.Debugln("value of the option is ", list_value)
	if reflect.TypeOf(list_value).Kind() != reflect.Slice {
		Log.Fatal("TestOption: Returned object with wrong type: ", reflect.TypeOf(value).Kind())
	}
}

// TODO: generate a sample json file
func TestConfig(t *testing.T) {
	err := Config.Init("conf.json")
	ErrCheckFatal(err, "Config file error")
	Log.Infoln("Json configuration: ", *Config.JsonConf)
	option, err := Config.GetOption("test.sub1.machines")
	Log.Infoln("test.sub1.machines: ", option)
	ErrCheckFatal(err, "Config GetOption error")
	if reflect.TypeOf(option).Kind() != reflect.Slice {
		Log.Fatal("TestConfig: GetOption should return a slice")
	}
	if option.([]interface{})[0] != "127.0.0.1:2379" {
		Log.Fatal("TestConfig: GetOption failed")
	}
}

func TestBroadcaster(t *testing.T) {
	b := NewBroadcaster(2)
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(b *Broadcaster, done chan bool, id int) {
			ch := b.Listen()
			if <-ch != true {
				Log.Fatal("Broadcaster failed")
			}
			if <-ch != false {
				Log.Fatal("Broadcaster failed")
			}
			done <- true
		}(b, done, i)
	}
	for err := b.Write(true, 2); err != nil; {
		time.Sleep(100 * time.Millisecond)
		err = b.Write(true, 2)
	}
	for err := b.Write(false, 2); err != nil; {
		time.Sleep(100 * time.Millisecond)
		err = b.Write(false, 2)
	}
	for i := 0; i < 100; i++ {
		<-done
	}
	Log.Info("Broadcaster test done.")
}
