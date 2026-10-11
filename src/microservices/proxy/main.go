package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
)

// Proxy Service (API Gateway) — реализация паттерна Strangler Fig.
//
// Прокси стоит перед монолитом и микросервисами и выступает единой точкой
// вызова. Трафик домена movies переключается с монолита на микросервис
// постепенно, по проценту миграции (фича-флаг MOVIES_MIGRATION_PERCENT):
// часть запросов уходит в новый сервис, остальная — в монолит.
// Так переход происходит незаметно для пользователей.

type Config struct {
	Port                string
	MonolithURL         string
	MoviesServiceURL    string
	EventsServiceURL    string
	GradualMigration    bool
	MoviesMigrationPct  int
}

func loadConfig() Config {
	return Config{
		Port:               getEnv("PORT", "8000"),
		MonolithURL:        getEnv("MONOLITH_URL", "http://monolith:8080"),
		MoviesServiceURL:   getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"),
		EventsServiceURL:   getEnv("EVENTS_SERVICE_URL", "http://events-service:8082"),
		GradualMigration:   getEnv("GRADUAL_MIGRATION", "true") == "true",
		MoviesMigrationPct: atoiDefault(getEnv("MOVIES_MIGRATION_PERCENT", "100"), 100),
	}
}

func main() {
	cfg := loadConfig()

	monolith := newBackend(cfg.MonolithURL)
	movies := newBackend(cfg.MoviesServiceURL)
	events := newBackend(cfg.EventsServiceURL)

	// Счётчик маршрутизации домена movies — для логирования перехода.
	var moviesTotal, moviesToService uint64

	mux := http.NewServeMux()

	// Health-чек прокси.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, "Strangler Fig Proxy is healthy")
	})

	// Домен movies: постепенное переключение трафика с монолита на микросервис.
	mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		useService := cfg.routeMoviesToService()
		atomic.AddUint64(&moviesTotal, 1)
		if useService {
			atomic.AddUint64(&moviesToService, 1)
		}
		log.Printf("movies route: %s %s -> %s (migration %d%%, sent to movies-service %d/%d)",
			r.Method, r.URL.RequestURI(), backendName(useService, movies, monolith),
			cfg.MoviesMigrationPct, atomic.LoadUint64(&moviesToService), atomic.LoadUint64(&moviesTotal))
		proxyTo(useService, movies, monolith)(w, r)
	})
	// Всё остальное внутри /api/movies (например /api/movies/health) идёт тем же правилом.
	mux.HandleFunc("/api/movies/", func(w http.ResponseWriter, r *http.Request) {
		useService := cfg.routeMoviesToService()
		proxyTo(useService, movies, monolith)(w, r)
	})

	// Домены, ещё не выделенные из монолита.
	mux.HandleFunc("/api/users", proxyTo(false, movies, monolith))
	mux.HandleFunc("/api/payments", proxyTo(false, movies, monolith))
	mux.HandleFunc("/api/subscriptions", proxyTo(false, movies, monolith))

	// Микросервис событий (Kafka).
	mux.Handle("/api/events", events.handler)
	mux.Handle("/api/events/", events.handler)

	log.Printf("proxy-service (Strangler Fig) listening on :%s, movies migration: gradual=%v percent=%d",
		cfg.Port, cfg.GradualMigration, cfg.MoviesMigrationPct)
	log.Printf("backends: monolith=%s movies=%s events=%s",
		cfg.MonolithURL, cfg.MoviesServiceURL, cfg.EventsServiceURL)

	if err := http.ListenAndServe(":"+cfg.Port, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// routeMoviesToService решает, куда направить очередной запрос домена movies.
// При включённом постепенном режиме — вероятностно, по проценту миграции;
// при выключенном — весь трафик домена уходит в микросервис (миграция завершена).
func (c Config) routeMoviesToService() bool {
	if !c.GradualMigration {
		return true
	}
	return rand.Intn(100) < c.MoviesMigrationPct
}

type backend struct {
	name    string
	url     *url.URL
	handler http.Handler
}

func newBackend(rawURL string) *backend {
	u, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("invalid backend url %q: %v", rawURL, err)
	}
	rp := httputil.NewSingleHostReverseProxy(u)
	original := rp.Director
	rp.Director = func(req *http.Request) {
		original(req)
		req.Host = u.Host
	}
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error for %s %s: %v", r.Method, r.URL.Path, err)
		http.Error(w, fmt.Sprintf(`{"error":%q}`, "upstream service unavailable"), http.StatusBadGateway)
	}
	return &backend{name: strings.TrimPrefix(u.Host, "http://"), url: u, handler: rp}
}

// proxyTo возвращает handler, направляющий запрос одному из двух бэкендов.
func proxyTo(useService bool, movies, monolith *backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if useService {
			movies.handler.ServeHTTP(w, r)
			return
		}
		monolith.handler.ServeHTTP(w, r)
	}
}

func backendName(useService bool, movies, monolith *backend) string {
	if useService {
		return movies.name
	}
	return monolith.name
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func atoiDefault(s string, defaultValue int) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return defaultValue
		}
		n = n*10 + int(c-'0')
	}
	if n > 100 {
		return 100
	}
	return n
}
