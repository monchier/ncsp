package ncsp

import (
	"reflect"
	"testing"
)

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
		Log.Fatal("TestOption: Returned object with wrong type")
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
