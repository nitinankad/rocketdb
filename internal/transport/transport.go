package transport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Request struct {
	Method string            `json:"method"`
	Path   string            `json:"path"`
	Query  map[string]string `json:"query,omitempty"`
	Body   json.RawMessage   `json:"body,omitempty"`
}

type Response struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body,omitempty"`
}

type Handler func(context.Context, Request) (Response, error)

type Server struct {
	handler Handler
}

func NewServer(handler Handler) *Server {
	return &Server{handler: handler}
}

func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()

	for {
		var req Request
		if err := readFrame(conn, &req); err != nil {
			if !isConnClosed(err) {
				_ = writeFrame(conn, Response{
					Status: http.StatusBadGateway,
					Body:   mustJSON(map[string]string{"error": err.Error()}),
				})
			}
			return
		}

		resp, err := s.handler(context.Background(), req)
		if err != nil {
			resp = Response{
				Status: http.StatusBadGateway,
				Body:   mustJSON(map[string]string{"error": err.Error()}),
			}
		}
		if resp.Status == 0 {
			resp.Status = http.StatusOK
		}

		if err := writeFrame(conn, resp); err != nil {
			return
		}
	}
}

type Client struct {
	addr    string
	timeout time.Duration
	pool    chan net.Conn
	mu      sync.Mutex
	closed  bool
}

func NewClient(addr string, timeout time.Duration, poolSize int) *Client {
	if poolSize <= 0 {
		poolSize = 1
	}
	return &Client{
		addr:    addr,
		timeout: timeout,
		pool:    make(chan net.Conn, poolSize),
	}
}

func (c *Client) Do(ctx context.Context, req Request) (Response, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return Response{}, err
	}

	deadline := time.Now().Add(c.timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return Response{}, err
	}

	if err := writeFrame(conn, req); err != nil {
		conn.Close()
		return Response{}, err
	}

	var resp Response
	if err := readFrame(conn, &resp); err != nil {
		conn.Close()
		return Response{}, err
	}

	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return Response{}, err
	}
	c.putConn(conn)
	return resp, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.pool)
	for conn := range c.pool {
		_ = conn.Close()
	}
}

func (c *Client) getConn(ctx context.Context) (net.Conn, error) {
	select {
	case conn := <-c.pool:
		if conn != nil {
			return conn, nil
		}
	default:
	}

	var d net.Dialer
	return d.DialContext(ctx, "tcp", dialAddr(c.addr))
}

func (c *Client) putConn(conn net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = conn.Close()
		return
	}
	select {
	case c.pool <- conn:
	default:
		_ = conn.Close()
	}
}

func DispatchHTTP(ctx context.Context, handler http.Handler, req Request) (Response, error) {
	target := req.Path
	if target == "" {
		target = "/"
	}

	if len(req.Query) > 0 {
		values := url.Values{}
		for k, v := range req.Query {
			values.Set(k, v)
		}
		target += "?" + values.Encode()
	}

	var body io.Reader
	if len(req.Body) > 0 {
		body = strings.NewReader(string(req.Body))
	}

	httpReq := httptest.NewRequest(req.Method, target, body).WithContext(ctx)
	if len(req.Body) > 0 {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httpReq)

	return Response{
		Status: rec.Code,
		Body:   append([]byte(nil), rec.Body.Bytes()...),
	}, nil
}

func readFrame(r io.Reader, dst any) error {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return err
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	if err := json.Unmarshal(payload, dst); err != nil {
		return fmt.Errorf("decode frame: %w", err)
	}
	return nil
}

func writeFrame(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(payload))); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

func dialAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}

func isConnClosed(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF {
		return true
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "closed") || strings.Contains(msg, "reset by peer")
}

func mustJSON(v any) json.RawMessage {
	payload, _ := json.Marshal(v)
	return payload
}
