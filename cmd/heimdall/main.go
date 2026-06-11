package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"heimdall/config"
	"heimdall/internal/broker"
	"heimdall/internal/monitor"
	"heimdall/internal/network"
)

func main() {
	cfg, err := config.Load("heimdall.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	b := broker.New(cfg)

	tcpServer := network.NewServer(cfg.ListenAddr, b)
	httpServer := monitor.NewServer(cfg.MonitorAddr, b)

	go func() {
		if err := tcpServer.Start(); err != nil {
			log.Fatalf("TCP server error: %v", err)
		}
	}()

	go func() {
		if err := httpServer.Start(); err != nil {
			log.Fatalf("HTTP monitor error: %v", err)
		}
	}()

	log.Printf("Heimdall broker started on %s", cfg.ListenAddr)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down...")
	b.Close()
}
