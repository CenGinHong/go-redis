package main

import (
	"log"

	"golang.org/x/sys/unix"
)

const BACKLOG int = 64

func Accept(fd int) (int, error) {
	nfd, _, err := unix.Accept(fd)
	// ignore client addr for now
	// 忽略端口
	return nfd, err
}

func Connect(host [4]byte, port int) (int, error) {
	s, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("init socket err: %v\n", err)
		return -1, err
	}
	addr := unix.SockaddrInet4 {
		Addr: host,
		Port: port,
	}
	if err = unix.Connect(s, &addr); err != nil {
		log.Printf("connect err: %v\n", err)
		return -1, err
	}
	return s, nil
}

func Read(fd int, buf []byte) (int, error) {
	return unix.Read(fd, buf)
}

func Write(fd int, buf []byte) (int, error) {
	return unix.Write(fd, buf)
}

// TcpServer 监听端口，并返回一个fd
func TcpServer(port int) (int, error) {
	s, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("init socket err: %v\n", err)
		return -1, nil
	}
	defer func() {
		if err != nil {
			unix.Close(s)
		}
	}()
	if err = unix.SetsockoptInt(s, unix.SOL_SOCKET, unix.SO_REUSEADDR, port); err != nil {
		log.Printf("set SO_REUSEADDR err: %v\n", err)
		return -1, nil
	}
	// golang.syscall will handle htons
	// 这个函数已经完成大小段转换的问题
	// golang will set addr.Addr = interface{}(0)
	addr := unix.SockaddrInet4{
		Port: port,
	}
	if err = unix.Bind(s, &addr); err != nil {
		log.Printf("bind addr err: %v\n", err)
		return -1, nil
	}
	if err = unix.Listen(s, BACKLOG); err != nil {
		log.Printf("listen socket err: %v\n", err)
		return -1, nil
	}
	return s, nil

}
