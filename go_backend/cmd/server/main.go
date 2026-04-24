package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"smart_city_backend/internal/api"
	"smart_city_backend/internal/consumer"
	"smart_city_backend/internal/db"
	"smart_city_backend/internal/models"
	"smart_city_backend/internal/processor"
)

func main() {
	// ── Config ────────────────────────────────────────────────────────────────
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	dbDSN := getEnv("DATABASE_URL", "postgres://smartcity:smartcity@localhost:5432/smartcity?sslmode=disable")
	apiPort := getEnv("API_PORT", ":8080")

	// ── Database ──────────────────────────────────────────────────────────────
	database, err := db.NewDB(dbDSN)
	if err != nil {
		log.Fatalf("[Main] Failed to connect to database: %v", err)
	}
	defer database.Close()

	if err := database.Migrate(); err != nil {
		log.Fatalf("[Main] Migration failed: %v", err)
	}

	// ── Processor ─────────────────────────────────────────────────────────────
	proc := processor.NewProcessor(processor.Callbacks{
		OnAlert:        func(a models.Alert) { database.InsertAlert(a) },
		OnWriteTraffic: func(r models.TrafficReading) { database.InsertTraffic(r) },
		OnWriteAQ:      func(r models.AirQualityReading) { database.InsertAirQuality(r) },
		OnWriteWeather: func(r models.WeatherReading) { database.InsertWeather(r) },
	})

	// ── Kafka Consumer ────────────────────────────────────────────────────────
	kc := consumer.NewKafkaConsumer(
		[]string{kafkaBrokers},
		consumer.Handlers{
			OnTraffic:    proc.HandleTraffic,
			OnAirQuality: proc.HandleAirQuality,
			OnWeather:    proc.HandleWeather,
		},
	)

	// ── API Server ────────────────────────────────────────────────────────────
	server := api.NewServer(proc)

	// ── Graceful Shutdown ─────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consumers in background
	go func() {
		log.Printf("[Main] Starting Kafka consumers...")
		kc.Start(ctx)
	}()

	// Start API server in background
	go func() {
		log.Printf("[Main] API server listening on %s", apiPort)
		if err := server.Run(apiPort); err != nil {
			log.Printf("[Main] API server error: %v", err)
		}
	}()

	log.Printf("[Main] Smart City Backend ready")
	log.Printf("[Main] Endpoints: http://localhost%s/api/v1/", apiPort)

	// Wait for shutdown signal
	<-sigChan
	log.Println("[Main] Shutdown signal received")
	cancel()
	kc.Shutdown()
	log.Println("[Main] Graceful shutdown complete")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
