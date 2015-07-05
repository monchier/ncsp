package ncsp

import (
	"github.com/coreos/go-etcd/etcd"
	"log"
)

func ErrCheckFatal(err error, msg string) {
	if err != nil {
		log.Fatal(msg, " : ", err)
	}
}

func EtcdErrorCode(err error) int {
	return err.(*etcd.EtcdError).ErrorCode
}
