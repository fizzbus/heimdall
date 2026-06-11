package network

import (
	"log"
	"net"
	"sync"

	"heimdall/internal/broker"
)

// Server — TCP-сервер брокера.
type Server struct {
	addr   string
	broker *broker.Broker
	ln     net.Listener
	wg     sync.WaitGroup
	quit   chan struct{}
}

// NewServer создаёт новый TCP-сервер.
func NewServer(addr string, b *broker.Broker) *Server {
	return &Server{
		addr:   addr,
		broker: b,
		quit:   make(chan struct{}),
	}
}

// Start запускает прослушивание входящих соединений.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	log.Printf("[network] listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil // штатное завершение
			default:
				log.Printf("[network] accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			NewHandler(c, s.broker).Handle()
		}(conn)
	}
}

// Stop корректно останавливает сервер: закрывает listener и ждёт завершения всех обработчиков.
func (s *Server) Stop() {
	close(s.quit)
	s.ln.Close()
	s.wg.Wait()
	log.Println("[network] server stopped")
}
