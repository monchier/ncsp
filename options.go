package ncsp

// TODO: refelct.Kind vs reflect.Type
// TODO: add support for lists
// TODO: each channel => its own option

import (
	"reflect"
)

type Options struct {
	values map[string]interface{}
	kinds  map[string]reflect.Kind
}

func NewOptions() *Options {
	var o Options
	o.values = make(map[string]interface{})
	o.kinds = make(map[string]reflect.Kind)
	return &o
}

func (o *Options) AddOption(name string, optType reflect.Kind) {
	o.kinds[name] = optType
}

func (o Options) GetOptionType(name string) reflect.Kind {
	return o.kinds[name]
}

func (o *Options) SetOption(name string, value interface{}) error {
	if o.kinds[name] != reflect.TypeOf(value).Kind() {
		return NewOptionError("Bad type")
	}
	o.values[name] = value
	return nil
}

func (o Options) GetOption(name string) interface{} {
	return o.values[name]
}
