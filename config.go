package ncsp

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
)

var Config = &NcspConfig{
	JsonConf: new(interface{}),
	Port:     make(chan uint32),
}

type NcspConfig struct {
	JsonConf *interface{} // Configuration data structure
	Port     chan uint32  // Next available port
}

func (n *NcspConfig) Init(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		Log.Errorln("error opening: ", filename, " : ", err)
		return err
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(n.JsonConf)
	if err != nil {
		Log.Errorln("decoding config file error: ", err)
		return err
	}
	go n.generatePortLoop()
	return nil
}

// TODO: types, for now it returns an interface
// TODO: error handling, add validation
func (n NcspConfig) GetOption(name string) (interface{}, error) {
	tokens := strings.Split(name, ".")
	// TODO: check *.JsonConf is of the right type
	m := (*n.JsonConf).(map[string]interface{})

	for i := range tokens {
		// TODO: check tokens[i] is in m.keys()
		if reflect.TypeOf(m[tokens[i]]).Kind() == reflect.Map {
			m = (m[tokens[i]]).(map[string]interface{})
		} else {
			return m[tokens[i]], nil

		}
	}

	Log.Errorln("GetOption error")
	return "", NewConfigError("GetOption error")
}

// TODO: temporary workaround
func ToEtcdMachinesList(in []interface{}) []string {
	out := make([]string, 0)
	for i := range in {
		out = append(out, "http://"+in[i].(string))
	}
	return out
}

func (n *NcspConfig) generatePortLoop() {
	// FIXME: need wrapper/ validators
	option, err := Config.GetOption("ncsp.base_port")
	ErrCheckFatal(err, "Configuration error")
	base_port := uint32(option.(float64))
	option, err = Config.GetOption("ncsp.max_ports")
	ErrCheckFatal(err, "Configuration error")
	max_ports := uint32(option.(float64))
	for {
		for i := base_port; i < base_port+max_ports; i++ {
			n.Port <- i
		}
	}
}
