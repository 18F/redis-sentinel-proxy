package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

var (
	masterAddr     *net.TCPAddr
	prevMasterAddr *net.TCPAddr
	raddr          *net.TCPAddr
	saddr          *net.TCPAddr

	localAddr    = flag.String("listen", ":9999", "local address")
	sentinelAddr = flag.String("sentinel", ":26379", "remote address")
	masterName   = flag.String("master", "", "name of the master redis node")
)

func main() {
	flag.Parse()

	laddr, err := net.ResolveTCPAddr("tcp", *localAddr)
	if err != nil {
		log.Fatalf("Failed to resolve local address: %s", err)
	}
	saddr, err = net.ResolveTCPAddr("tcp", *sentinelAddr)
	if err != nil {
		log.Fatalf("Failed to resolve sentinel address: %s", err)
	}

	stopChan := make(chan struct{})
	go master(&stopChan)

	listener, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Println(err)
			continue
		}
		go proxy(conn, masterAddr, &stopChan)
	}
}

func master(stopChan *chan struct{}) {
	var err error
	for {
		// has master changed from last time?
		masterAddr, err = getMasterAddr(saddr, *masterName)
		if err != nil {
			log.Println("Error returned after the call to func getMasterAddr")
			log.Println(err)
		}
		if masterAddr.String() != prevMasterAddr.String() {
			fmt.Printf("Master Address changed. Closing stopChan. Setting masterAddr to %s from %s \n", masterAddr.String(), prevMasterAddr.String())
			close(*stopChan)
			*stopChan = make(chan struct{})
		}
		prevMasterAddr = masterAddr
		time.Sleep(1 * time.Second)
	}
}

func pipe(r io.Reader, w io.WriteCloser) {
	io.Copy(w, r)
	w.Close()
	fmt.Println("Closing pipe")
}

// pass a stopChan to the go routine
func proxy(local *net.TCPConn, remoteAddr *net.TCPAddr, stopChan *chan struct{}) {
	fmt.Printf("Opening a new connection on remoteAddr, %s\n", remoteAddr)
	remote, err := net.DialTCP("tcp", nil, remoteAddr)
	if err != nil {
		log.Println("Error occurred in the DialTCP within func proxy")
		log.Println(err)
		local.Close()
		return
	}
	go pipe(local, remote)
	go pipe(remote, local)
	<-*stopChan // read from stopChan
	fmt.Println("Closing Proxy")
	local.Close()
}

func getMasterAddr(sentinelAddress *net.TCPAddr, masterName string) (*net.TCPAddr, error) {
	log.Printf("Polling for changes to master IP address {prev: %s, current: %s}\n", prevMasterAddr, masterAddr)
	conn, err := net.DialTCP("tcp", nil, sentinelAddress)
	if err != nil {
		log.Printf("Error occurred in the DialTCP to sentinelAddress (%s) within func getMasterAddr\n", sentinelAddress)
		return nil, err
	}

	defer conn.Close()

	conn.Write([]byte(fmt.Sprintf("sentinel get-master-addr-by-name %s\n", masterName)))

	b := make([]byte, 256)
	_, err = conn.Read(b)
	if err != nil {
		log.Fatal(err)
	}

	parts := strings.Split(string(b), "\r\n")

	if len(parts) < 5 {
		err = errors.New("Couldn't get master address from sentinel")
		return nil, err
	}

	//getting the string address for the master node
	stringaddr := fmt.Sprintf("%s:%s", parts[2], parts[4])
	addr, err := net.ResolveTCPAddr("tcp", stringaddr)

	if err != nil {
		log.Printf("Error occurred in the ResolveTCPAddr to stringaddr (%s) within func getMasterAddr\n", stringaddr)
		return nil, err
	}

	//check that there's actually someone listening on that address
	conn2, err := net.DialTCP("tcp", nil, addr)
	if err == nil {
		defer conn2.Close()
	}

	if err != nil {
		log.Printf("Error occurred in the DialTCP to addr (%s) within func getMasterAddr\n", addr)
	}

	return addr, err
}
