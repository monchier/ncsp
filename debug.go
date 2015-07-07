package ncsp

import (
	"github.com/Sirupsen/logrus"
	"os"
)

var Log = &logrus.Logger{
	Out: os.Stderr,
	//	Formatter: new(logrus.JSONFormatter),
	Formatter: new(logrus.TextFormatter),
	Hooks:     make(logrus.LevelHooks),
	Level:     logrus.DebugLevel,
}
