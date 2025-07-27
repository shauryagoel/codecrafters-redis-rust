// Run using-
// go run benchmark_redis.go --host localhost --port 6379 --total-requests 1000000 --clients 200
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	host          = flag.String("host", "127.0.0.1", "Redis server host")
	port          = flag.Int("port", 6379, "Redis server port")
	totalRequests = flag.Int("total-requests", 0, "Total number of commands to send (required)")
	clients       = flag.Int("clients", 0, "Number of concurrent clients (required)")
)

// respPing is the raw Redis PING command in RESP format
var respPing = []byte("*1\r\n$4\r\nPING\r\n")

func client(host string, port int, n int, wg *sync.WaitGroup) {
	defer wg.Done()

	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Error connecting to %s: %v\n", addr, err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for i := 0; i < n; i++ {
		// send PING
		if _, err := conn.Write(respPing); err != nil {
			fmt.Printf("Write error: %v\n", err)
			return
		}
		// read response line
		if _, err := reader.ReadBytes('\n'); err != nil {
			fmt.Printf("Read error: %v\n", err)
			return
		}
	}
}

func main() {
	flag.Parse()

	if *totalRequests <= 0 || *clients <= 0 {
		flag.Usage()
		return
	}

	// divide requests among clients
	perClient := *totalRequests / *clients
	extra := *totalRequests % *clients

	var wg sync.WaitGroup
	wg.Add(*clients)

	start := time.Now()

	for i := 0; i < *clients; i++ {
		cnt := perClient
		if i < extra {
			cnt++
		}
		go client(*host, *port, cnt, &wg)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Total requests : %d\n", *totalRequests)
	fmt.Printf("Total clients  : %d\n", *clients)
	fmt.Printf("Elapsed time   : %.3f s\n", elapsed.Seconds())
	ops := float64(*totalRequests) / elapsed.Seconds()
	fmt.Printf("Throughput     : %.0f ops/sec\n", ops)
}
