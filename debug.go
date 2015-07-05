package ncsp

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

var logger *ncspLogger

func SetLogger(l *log.Logger) {
	logger = &ncspLogger{l}
}

func GetLogger() *log.Logger {
	return logger.log
}

type ncspLogger struct {
	log *log.Logger
}

func (p *ncspLogger) Debug(args ...interface{}) {
	msg := "DEBUG: " + fmt.Sprint(args...)
	p.log.Println(msg)
}

func (p *ncspLogger) Debugf(f string, args ...interface{}) {
	msg := "DEBUG: " + fmt.Sprintf(f, args...)
	// Append newline if necessary
	if !strings.HasSuffix(msg, "\n") {
		msg = msg + "\n"
	}
	p.log.Print(msg)
}

func (p *ncspLogger) Warning(args ...interface{}) {
	msg := "WARNING: " + fmt.Sprint(args...)
	p.log.Println(msg)
}

func (p *ncspLogger) Warningf(f string, args ...interface{}) {
	msg := "WARNING: " + fmt.Sprintf(f, args...)
	// Append newline if necessary
	if !strings.HasSuffix(msg, "\n") {
		msg = msg + "\n"
	}
	p.log.Print(msg)
}

func init() {
	// Default logger uses the go default log.
	SetLogger(log.New(ioutil.Discard, "ncsp", log.LstdFlags))
}
