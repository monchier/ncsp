package ncsp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
)

func ReceiveMessage(conn net.Conn, buf *bytes.Buffer) error {
	var length uint32
	err := binary.Read(conn, binary.LittleEndian, &length)
	if err != nil {
		log.Println("binary.Read failed: ", err)
	}
	n, err := io.CopyN(buf, conn, int64(length))
	if err != nil {
		log.Println("io.CopyN failed: ", err)
	}
	if uint32(n) != length {
		log.Panic("io.CopyN didn't return all bytes")
	}
	return err
}

func SendMessage(conn net.Conn, body *bytes.Buffer) error {
	length := uint32(body.Len())
	err := binary.Write(conn, binary.LittleEndian, length)
	if err != nil {
		log.Println("binary.Write failed:", err)
	}
	n, err := io.Copy(conn, bufio.NewReader(body))
	if err != nil {
		log.Println("io.Copy failed: ", err)
	}
	if uint32(n) != length {
		log.Panic("io.Copy didn't return all bytes")
	}
	return err
}
