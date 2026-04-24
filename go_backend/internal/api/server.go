package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"smart_city_backend/internal/processor"
)

type Server struct {
	proc   *processor.Processor
	router *gin.Engine
}

func NewServer(proc *processor.Processor) *Server {
	s := &Server{proc: proc}
	s.router = gin.Default()
	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	// Health check
	s.router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	v1 := s.router.Group("/api/v1")
	{
		// ── Traffic ─────────────────────────────────────────────
		v1.GET("/traffic/latest", s.getTrafficLatest)

		// ── Air Quality ──────────────────────────────────────────
		v1.GET("/air/latest", s.getAirLatest)

		// ── Weather ──────────────────────────────────────────────
		v1.GET("/weather/latest", s.getWeatherLatest)

		// ── Alerts ───────────────────────────────────────────────
		v1.GET("/alerts", s.getAlerts)

		// ── Correlation ──────────────────────────────────────────
		v1.GET("/correlation", s.getCorrelation)
		v1.GET("/correlation/stats", s.getCorrelationStats)
	}
}

func (s *Server) Run(addr string) error {
	return s.router.Run(addr)
}

// GET /api/v1/traffic/latest
func (s *Server) getTrafficLatest(c *gin.Context) {
	data := s.proc.GetLatestTraffic()
	c.JSON(http.StatusOK, gin.H{
		"count": len(data),
		"data":  data,
	})
}

// GET /api/v1/air/latest
func (s *Server) getAirLatest(c *gin.Context) {
	data := s.proc.GetLatestAirQuality()
	c.JSON(http.StatusOK, gin.H{
		"count": len(data),
		"data":  data,
	})
}

// GET /api/v1/weather/latest
func (s *Server) getWeatherLatest(c *gin.Context) {
	data := s.proc.GetLatestWeather()
	c.JSON(http.StatusOK, gin.H{
		"count": len(data),
		"data":  data,
	})
}

// GET /api/v1/alerts?limit=20&severity=high
func (s *Server) getAlerts(c *gin.Context) {
	limit := 20
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	severity := c.Query("severity") // optional filter

	alerts := s.proc.GetAlerts(limit)
	if severity != "" {
		filtered := alerts[:0]
		for _, a := range alerts {
			if a.Severity == severity {
				filtered = append(filtered, a)
			}
		}
		alerts = filtered
	}

	c.JSON(http.StatusOK, gin.H{
		"count": len(alerts),
		"data":  alerts,
	})
}

// GET /api/v1/correlation?limit=200
func (s *Server) getCorrelation(c *gin.Context) {
	limit := 200
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	data := s.proc.GetCorrelationPoints(limit)
	c.JSON(http.StatusOK, gin.H{
		"count": len(data),
		"data":  data,
	})
}

// GET /api/v1/correlation/stats
func (s *Server) getCorrelationStats(c *gin.Context) {
	stats := s.proc.ComputeCorrelationStats()
	c.JSON(http.StatusOK, stats)
}
