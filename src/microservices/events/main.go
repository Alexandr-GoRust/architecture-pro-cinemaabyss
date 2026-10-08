// Events Service — MVP проверки гипотезы применения Kafka в архитектуре
// «Кинобездны».
//
// Сервис принимает по REST события трёх доменов (movie/user/payment),
// публикует их в топики Kafka (producer) и сам же читает (consumer),
// обрабатывая каждое событие записью в лог сервиса.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	
	"time"

	"github.com/segmentio/kafka-go"
)

// Event — унифицированная обёртка события (см. api-specification.yaml).
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Timestamp string                 `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

// EventResponse — ответ API после успешной публикации в Kafka.
type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type domainConfig struct {
	topic      string
	idField    string // поле полезной нагрузки, из которого собирается id события
	requireds  []string
}

var domains = map[string]domainConfig{
	"movie":   {topic: "movie-events", idField: "movie_id", requireds: []string{"movie_id", "title", "action"}},
	"user":    {topic: "user-events", idField: "user_id", requireds: []string{"user_id", "action", "timestamp"}},
	"payment": {topic: "payment-events", idField: "payment_id", requireds: []string{"payment_id", "user_id", "amount", "status", "timestamp"}},
}

var brokers []string

func main() {
	brokers = []string{getEnv("KAFKA_BROKERS", "kafka:9092")}
	port := getEnv("PORT", "8082")
	groupID := getEnv("KAFKA_GROUP_ID", "events-service")

	// Consumer: читаем все доменные топики и логируем каждое событие.
	// Каждому топику — своя consumer-группа (один читатель в группе):
	// подписка стабильна и группа не уходит в бесконечный ребаланс.
	for _, d := range domains {
		go consume(d.topic, groupID+"-"+d.topic)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", handleHealth)
	mux.HandleFunc("/api/events/movie", eventHandler("movie"))
	mux.HandleFunc("/api/events/user", eventHandler("user"))
	mux.HandleFunc("/api/events/payment", eventHandler("payment"))

	log.Printf("events-service listening on :%s, kafka brokers: %v", port, brokers)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

// eventHandler валидирует тело запроса, публикует событие в Kafka
// и отвечает метаданными записи (партиция и offset).
func eventHandler(domain string) http.HandlerFunc {
	d := domains[domain]

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
			return
		}
		for _, field := range d.requireds {
			if _, ok := payload[field]; !ok {
				http.Error(w, fmt.Sprintf(`{"error":%q}`, "missing required field: "+field), http.StatusBadRequest)
				return
			}
		}

		event := Event{
			ID:        fmt.Sprintf("%v-%v", domain, payload[d.idField]),
			Type:      domain,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Payload:   payload,
		}
		value, _ := json.Marshal(event)

		msg := kafka.Message{
			Topic: d.topic,
			Key:   []byte(event.ID),
			Value: value,
		}

		// Для MVP публикуем через одноразовый writer: создаём под запрос,
		// закрываем сразу после записи — соединение с брокером не висит.
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers:      brokers,
			Balancer:     &kafka.Hash{},
			RequiredAcks: 1, // acks=1 — подтверждение записи лидером партиции
			MaxAttempts:  3,
			Logger:       kafka.LoggerFunc(log.Printf),
		})
		defer writer.Close()

		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		if err := writer.WriteMessages(ctx, msg); err != nil {
			log.Printf("kafka write error: %v", err)
			http.Error(w, `{"error":"failed to publish event"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(EventResponse{
			Status:    "success",
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Event:     event,
		})
	}
}

// consume читает топик в группе и обрабатывает события (обработка MVP — лог).
func consume(topic, groupID string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("kafka read error (topic %s): %v", topic, err)
			time.Sleep(3 * time.Second)
			continue
		}
		// Обработка события: раскладываем полезную нагрузку в лог сервиса.
		var ev Event
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			log.Printf("[%s] partition=%d offset=%d raw=%s", topic, m.Partition, m.Offset, string(m.Value))
			continue
		}
		log.Printf("[consumed] topic=%s partition=%d offset=%d event_id=%s type=%s payload=%s",
			topic, m.Partition, m.Offset, ev.ID, ev.Type, string(m.Value))
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

