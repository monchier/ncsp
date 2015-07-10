package ncsp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
)

// ReceiveMessage receives a message
func ReceiveMessage(conn net.Conn, buf *bytes.Buffer) error {
	var length uint32
	err := binary.Read(conn, binary.LittleEndian, &length)
	if err != nil {
		log.Println("binary.Read failed: ", err)
	}
	if length == 0 {
		return nil
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

// SendMessage sends a message
func SendMessage(conn net.Conn, body *bytes.Buffer) error {
	length := uint32(body.Len())
	err := binary.Write(conn, binary.LittleEndian, length)
	if err != nil {
		log.Println("SendMessage: binary.Write failed:", err)
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

// SendZero sends an empty message
func SendZero(conn net.Conn) error {
	err := binary.Write(conn, binary.LittleEndian, uint32(0))
	if err != nil {
		log.Println("SendZero: binary.Write failed:", err)
	}
	return err
}
