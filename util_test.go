package ncsp

import (
	"log"
	"reflect"
	"testing"
)

func TestOptions(t *testing.T) {
	log.Println("Testing options.go")
	opts := NewOptions()
	opts.AddOption("test", reflect.Bool)
	err := opts.SetOption("test", 123)
	log.Println("Error: ", err, " type: ", reflect.TypeOf(err))
	if err == nil {
		log.Fatal("It should not be possible to set a boolean ",
			"option with an integer")
	}
	err = opts.SetOption("test", true)
	if err != nil {
		log.Fatal("Failed to set option", err)
	}
	value := opts.GetOption("test")
	log.Println("value of the option is ", value)
	if reflect.TypeOf(value).Kind() != reflect.Bool {
		log.Fatal("Returned object with wrong type")
	}
}
