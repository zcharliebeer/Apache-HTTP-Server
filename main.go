package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"time"
)

// ConnState represents the connection state machine states.
type ConnState int

const (
	CONN_STATE_CHECK_REQUEST ConnState = iota
	CONN_STATE_READ_REQUEST
	CONN_STATE_HANDLER
	CONN_STATE_WRITE_COMPLETION
	CONN_STATE_SUSPENDED
	CONN_STATE_LINGER
	CONN_STATE_CLOSE
)

// ConnKeepAlive represents the keep-alive status of the connection.
type ConnKeepAlive int

const (
	AP_CONN_KEEPALIVE ConnKeepAlive = iota
	AP_CONN_CLOSE
)

// Connection represents a client connection and its associated resources.
type Connection struct {
	Socket    net.Conn
	State     ConnState
	KeepAlive ConnKeepAlive
	Pool      map[string]interface{} // Simulates c->pool
}

// NewConnection initializes a new Connection.
func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		Socket:    conn,
		State:     CONN_STATE_READ_REQUEST,
		KeepAlive: AP_CONN_KEEPALIVE,
		Pool:      make(map[string]interface{}),
	}
}

// Close closes the socket and destroys the connection pool to prevent resource leaks.
func (c *Connection) Close() { 
	if c.Socket != nil {
		_ = c.Socket.Close()
		c.Socket = nil
	}
	c.Pool = nil // Explicitly destroy the connection memory pool
}

// WorkerQueue represents the queue of idle worker threads/channels.
type WorkerQueue struct {
	queue chan chan *Connection
}

// NewWorkerQueue initializes a WorkerQueue.
func NewWorkerQueue(maxWorkers int) *WorkerQueue {
	return &WorkerQueue{
		queue: make(chan chan *Connection, maxWorkers),
	}
}

// Worker represents a worker thread.
type Worker struct {
	id          int
	workerQueue *WorkerQueue
	connChan    chan *Connection
	quit        chan struct{}
}

// NewWorker creates a new Worker.
func NewWorker(id int, wq *WorkerQueue) *Worker {
	return &Worker{
		id:          id,
		workerQueue: wq,
		connChan:    make(chan *Connection),
		quit:        make(chan struct{}),
	}
}

// Start starts the worker loop.
func (w *Worker) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Register this worker as idle by putting its connection channel into the worker queue
			select {
			case w.workerQueue.queue <- w.connChan:
			case <-w.quit:
				return
			}

			// Wait for a connection to process
			select {
			case c := <-w.connChan:
				if c == nil {
					continue
				}
				w.process(c)
			case <-w.quit:
				return
			}
		}
	}()
}

// Stop stops the worker.
func (w *Worker) Stop() {
	close(w.quit)
}

// process handles the connection processing.
func (w *Worker) process(c *Connection) {
	defer func() {
		// Ensure connection is closed and resources are released
		c.Close()
	}()

	for {
		// 1. Read Request
		buf := make([]byte, 4096)
		// Set a read deadline to detect timeouts
		_ = c.Socket.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := c.Socket.Read(buf)
		if err != nil {
			// Detect abrupt client disconnection (TCP FIN/RST) or timeout
			if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
				c.State = CONN_STATE_CLOSE
			} else {
				c.State = CONN_STATE_LINGER
			}
			c.KeepAlive = AP_CONN_CLOSE
			return // Immediately return to release the worker thread
		}

		if n == 0 {
			c.State = CONN_STATE_CLOSE
			c.KeepAlive = AP_CONN_CLOSE
			return
		}

		// 2. Process Request & Write Response
		response := []byte("HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\nHello, World!")
		_ = c.Socket.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err = c.Socket.Write(response)
		if err != nil {
			// Detect write failure due to broken pipe or connection reset
			if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
				c.State = CONN_STATE_CLOSE
			} else {
				c.State = CONN_STATE_LINGER
			}
			c.KeepAlive = AP_CONN_CLOSE
			return // Immediately return to release the worker thread
		}

		c.State = CONN_STATE_WRITE_COMPLETION

		// If keep-alive is disabled or connection is closing, exit loop
		if c.KeepAlive == AP_CONN_CLOSE || c.State == CONN_STATE_CLOSE {
			return
		}
	}
}

func main() {
	fmt.Println("Apache HTTP Server MPM Simulation in Go started.")
}
