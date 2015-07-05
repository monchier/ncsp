package ncsp

import (
	"errors"
	"github.com/coreos/go-etcd/etcd"
	"log"
)

type NcspError struct {
	err error
}

func (e NcspError) Error() string {
	return e.err.Error()
}

type OptionError struct {
	NcspError
}

func NewNcspError(msg string) *NcspError {
	var e NcspError
	e.err = errors.New(msg)
	return &e
}

func NewOptionError(msg string) *OptionError {
	var e OptionError
	e.err = errors.New(msg)
	return &e
}

func ErrCheckFatal(err error, msg string) {
	if err != nil {
		log.Fatal(msg, " : ", err)
	}
}

func EtcdErrorCode(err error) int {
	return err.(*etcd.EtcdError).ErrorCode
}
