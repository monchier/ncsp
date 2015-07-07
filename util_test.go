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
		Log.Fatal("It should not be possible to set a boolean ",
			"option with an integer")
	}
	err = opts.SetOption("test", true)
	if err != nil {
		Log.Fatal("Failed to set option", err)
	}
	value := opts.GetOption("test")
	Log.Debugln("value of the option is ", value)
	if reflect.TypeOf(value).Kind() != reflect.Bool {
		Log.Fatal("Returned object with wrong type")
	}
}
