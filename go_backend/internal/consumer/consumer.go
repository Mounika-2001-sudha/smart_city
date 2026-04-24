package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"smart_city_backend/internal/models"
)

const (
	TopicTraffic    = "traffic_stream"
	TopicAirQuality = "air_quality_stream"
	TopicWeather    = "weather_stream"
)

// Callbacks the processor registers to receive parsed events
type Handlers struct {
	OnTraffic    func(models.TrafficReading)
	OnAirQuality func(models.AirQualityReading)
	OnWeather    func(models.WeatherReading)
}

// KafkaConsumer manages concurrent consumption of all three streams
type KafkaConsumer struct {
	brokers  []string
	handlers Handlers
	readers  []*kafka.Reader
	mu       sync.Mutex
}

func NewKafkaConsumer(brokers []string, handlers Handlers) *KafkaConsumer {
	return &KafkaConsumer{
		brokers:  brokers,
		handlers: handlers,
	}
}

func (kc *KafkaConsumer) newReader(topic, groupID string) *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        kc.brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 << 20, // 10 MB
		MaxWait:        500 * time.Millisecond,
		StartOffset:    kafka.LastOffset,
		CommitInterval: time.Second,
	})
	kc.mu.Lock()
	kc.readers = append(kc.readers, r)
	kc.mu.Unlock()
	return r
}

// Start launches three goroutines, one per topic
func (kc *KafkaConsumer) Start(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(3)
	go func() { defer wg.Done(); kc.consumeTraffic(ctx) }()
	go func() { defer wg.Done(); kc.consumeAirQuality(ctx) }()
	go func() { defer wg.Done(); kc.consumeWeather(ctx) }()

	wg.Wait()
}

func (kc *KafkaConsumer) consumeTraffic(ctx context.Context) {
	r := kc.newReader(TopicTraffic, "go-backend-traffic")
	defer r.Close()
	log.Printf("[Consumer] Traffic stream goroutine started")

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[Consumer] Traffic read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var reading models.TrafficReading
		if err := json.Unmarshal(m.Value, &reading); err != nil {
			log.Printf("[Consumer] Traffic parse error: %v | raw: %s", err, string(m.Value[:min(80, len(m.Value))]))
			continue
		}

		// Normalize timestamp
		if reading.Timestamp.IsZero() {
			reading.Timestamp = time.Now().UTC()
		}

		if kc.handlers.OnTraffic != nil {
			kc.handlers.OnTraffic(reading)
		}
	}
}

func (kc *KafkaConsumer) consumeAirQuality(ctx context.Context) {
	r := kc.newReader(TopicAirQuality, "go-backend-aq")
	defer r.Close()
	log.Printf("[Consumer] Air quality stream goroutine started")

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[Consumer] AQ read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var reading models.AirQualityReading
		if err := json.Unmarshal(m.Value, &reading); err != nil {
			log.Printf("[Consumer] AQ parse error: %v", err)
			continue
		}

		if reading.Timestamp.IsZero() {
			reading.Timestamp = time.Now().UTC()
		}

		if kc.handlers.OnAirQuality != nil {
			kc.handlers.OnAirQuality(reading)
		}
	}
}

func (kc *KafkaConsumer) consumeWeather(ctx context.Context) {
	r := kc.newReader(TopicWeather, "go-backend-weather")
	defer r.Close()
	log.Printf("[Consumer] Weather stream goroutine started")

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[Consumer] Weather read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var reading models.WeatherReading
		if err := json.Unmarshal(m.Value, &reading); err != nil {
			log.Printf("[Consumer] Weather parse error: %v", err)
			continue
		}

		if reading.Timestamp.IsZero() {
			reading.Timestamp = time.Now().UTC()
		}

		if kc.handlers.OnWeather != nil {
			kc.handlers.OnWeather(reading)
		}
	}
}

// Shutdown closes all readers gracefully
func (kc *KafkaConsumer) Shutdown() {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	for _, r := range kc.readers {
		if err := r.Close(); err != nil {
			log.Printf("[Consumer] Error closing reader: %v", err)
		}
	}
	fmt.Println("[Consumer] All readers closed")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
