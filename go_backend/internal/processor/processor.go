package processor

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"smart_city_backend/internal/models"
)

// Alert thresholds
const (
	AQIHighThreshold         = 150.0
	AQICriticalThreshold     = 200.0
	CongestionHighThreshold  = 0.7
	PM25SpikeThreshold       = 100.0
	LowVisibilityThreshold   = 2.0 // km
)

// Processor holds in-memory state and processes incoming events
type Processor struct {
	mu sync.RWMutex

	// Latest readings per zone/station
	latestTraffic    map[string]models.TrafficReading
	latestAirQuality map[string]models.AirQualityReading
	latestWeather    map[string]models.WeatherReading

	// Rolling window (last 100 per zone for analytics)
	trafficWindow    map[string][]models.TrafficReading
	aqWindow         map[string][]models.AirQualityReading
	weatherWindow    map[string][]models.WeatherReading

	// Active alerts
	alerts []models.Alert
	nextID int64

	// DB writer callback (injected)
	onAlert        func(models.Alert)
	onWriteTraffic func(models.TrafficReading)
	onWriteAQ      func(models.AirQualityReading)
	onWriteWeather func(models.WeatherReading)
}

type Callbacks struct {
	OnAlert        func(models.Alert)
	OnWriteTraffic func(models.TrafficReading)
	OnWriteAQ      func(models.AirQualityReading)
	OnWriteWeather func(models.WeatherReading)
}

func NewProcessor(cb Callbacks) *Processor {
	return &Processor{
		latestTraffic:    make(map[string]models.TrafficReading),
		latestAirQuality: make(map[string]models.AirQualityReading),
		latestWeather:    make(map[string]models.WeatherReading),
		trafficWindow:    make(map[string][]models.TrafficReading),
		aqWindow:         make(map[string][]models.AirQualityReading),
		weatherWindow:    make(map[string][]models.WeatherReading),
		onAlert:          cb.OnAlert,
		onWriteTraffic:   cb.OnWriteTraffic,
		onWriteAQ:        cb.OnWriteAQ,
		onWriteWeather:   cb.OnWriteWeather,
	}
}

// ─── Traffic Processing ───────────────────────────────────────────────────

func (p *Processor) HandleTraffic(r models.TrafficReading) {
	p.mu.Lock()
	p.latestTraffic[r.Zone] = r
	window := append(p.trafficWindow[r.Zone], r)
	if len(window) > 100 {
		window = window[1:]
	}
	p.trafficWindow[r.Zone] = window
	p.mu.Unlock()

	// Async write to DB
	if p.onWriteTraffic != nil {
		go p.onWriteTraffic(r)
	}

	// Alert: congestion spike
	if r.CongestionIndex > CongestionHighThreshold {
		severity := "high"
		if r.CongestionIndex > 0.9 {
			severity = "critical"
		}
		p.raiseAlert(models.Alert{
			AlertType: "congestion",
			Severity:  severity,
			Zone:      r.Zone,
			Message:   fmt.Sprintf("High congestion in %s: index=%.2f, speed=%.1f km/h", r.Zone, r.CongestionIndex, r.AvgSpeedKmh),
			Value:     r.CongestionIndex,
			Threshold: CongestionHighThreshold,
			Timestamp: r.Timestamp,
		})
	}

	// Alert: traffic incident
	if r.IncidentFlag {
		p.raiseAlert(models.Alert{
			AlertType: "incident",
			Severity:  "medium",
			Zone:      r.Zone,
			Message:   fmt.Sprintf("Traffic incident reported in %s", r.Zone),
			Value:     r.CongestionIndex,
			Threshold: 0,
			Timestamp: r.Timestamp,
		})
	}
}

// ─── Air Quality Processing ───────────────────────────────────────────────

func (p *Processor) HandleAirQuality(r models.AirQualityReading) {
	p.mu.Lock()
	p.latestAirQuality[r.Station] = r
	window := append(p.aqWindow[r.Station], r)
	if len(window) > 100 {
		window = window[1:]
	}
	p.aqWindow[r.Station] = window
	p.mu.Unlock()

	if p.onWriteAQ != nil {
		go p.onWriteAQ(r)
	}

	// Alert: AQI thresholds
	if r.AQI > int(AQICriticalThreshold) {
		p.raiseAlert(models.Alert{
			AlertType: "aqi_spike",
			Severity:  "critical",
			Zone:      r.Station,
			Message:   fmt.Sprintf("CRITICAL air quality at %s: AQI=%d (%s), PM2.5=%.1f µg/m³", r.Station, r.AQI, r.AQICategory, r.PM25),
			Value:     float64(r.AQI),
			Threshold: AQICriticalThreshold,
			Timestamp: r.Timestamp,
		})
	} else if r.AQI > int(AQIHighThreshold) {
		p.raiseAlert(models.Alert{
			AlertType: "aqi_spike",
			Severity:  "high",
			Zone:      r.Station,
			Message:   fmt.Sprintf("Unhealthy air quality at %s: AQI=%d, PM2.5=%.1f µg/m³", r.Station, r.AQI, r.PM25),
			Value:     float64(r.AQI),
			Threshold: AQIHighThreshold,
			Timestamp: r.Timestamp,
		})
	}

	// Alert: combined traffic + pollution
	p.mu.RLock()
	trafficReading, hasTraffic := p.getZoneTrafficForStation(r.Station)
	p.mu.RUnlock()

	if hasTraffic && trafficReading.CongestionIndex > 0.6 && r.AQI > int(AQIHighThreshold) {
		p.raiseAlert(models.Alert{
			AlertType: "combined_hazard",
			Severity:  "high",
			Zone:      r.Station,
			Message:   fmt.Sprintf("Combined hazard: congestion=%.2f + AQI=%d in %s", trafficReading.CongestionIndex, r.AQI, r.Station),
			Value:     float64(r.AQI),
			Threshold: AQIHighThreshold,
			Timestamp: r.Timestamp,
		})
	}
}

// ─── Weather Processing ───────────────────────────────────────────────────

func (p *Processor) HandleWeather(r models.WeatherReading) {
	p.mu.Lock()
	p.latestWeather[r.Station] = r
	window := append(p.weatherWindow[r.Station], r)
	if len(window) > 100 {
		window = window[1:]
	}
	p.weatherWindow[r.Station] = window
	p.mu.Unlock()

	if p.onWriteWeather != nil {
		go p.onWriteWeather(r)
	}

	// Alert: low visibility
	if r.VisibilityKm < LowVisibilityThreshold {
		p.raiseAlert(models.Alert{
			AlertType: "low_visibility",
			Severity:  "medium",
			Zone:      r.Station,
			Message:   fmt.Sprintf("Low visibility at %s: %.1f km (%s)", r.Station, r.VisibilityKm, r.Condition),
			Value:     r.VisibilityKm,
			Threshold: LowVisibilityThreshold,
			Timestamp: r.Timestamp,
		})
	}
}

// ─── Alert Management ─────────────────────────────────────────────────────

func (p *Processor) raiseAlert(a models.Alert) {
	p.mu.Lock()
	p.nextID++
	a.ID = p.nextID
	// Keep last 200 alerts in memory
	p.alerts = append(p.alerts, a)
	if len(p.alerts) > 200 {
		p.alerts = p.alerts[1:]
	}
	p.mu.Unlock()

	log.Printf("[ALERT] [%s/%s] %s", a.Severity, a.AlertType, a.Message)

	if p.onAlert != nil {
		go p.onAlert(a)
	}
}

// ─── API Data Accessors ───────────────────────────────────────────────────

func (p *Processor) GetLatestTraffic() []models.TrafficLatest {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]models.TrafficLatest, 0, len(p.trafficWindow))
	for zone, window := range p.trafficWindow {
		if len(window) == 0 {
			continue
		}
		var sumCong, sumSpeed float64
		var totalVeh, incidents int
		for _, r := range window {
			sumCong += r.CongestionIndex
			sumSpeed += r.AvgSpeedKmh
			totalVeh += r.VehicleCount
			if r.IncidentFlag {
				incidents++
			}
		}
		n := float64(len(window))
		result = append(result, models.TrafficLatest{
			Zone:          zone,
			AvgCongestion: math.Round(sumCong/n*1000) / 1000,
			AvgSpeed:      math.Round(sumSpeed/n*10) / 10,
			TotalVehicles: totalVeh,
			IncidentCount: incidents,
			UpdatedAt:     window[len(window)-1].Timestamp,
		})
	}
	return result
}

func (p *Processor) GetLatestAirQuality() []models.AirQualityReading {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]models.AirQualityReading, 0, len(p.latestAirQuality))
	for _, r := range p.latestAirQuality {
		result = append(result, r)
	}
	return result
}

func (p *Processor) GetLatestWeather() []models.WeatherReading {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]models.WeatherReading, 0, len(p.latestWeather))
	for _, r := range p.latestWeather {
		result = append(result, r)
	}
	return result
}

func (p *Processor) GetAlerts(limit int) []models.Alert {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if limit <= 0 || limit > len(p.alerts) {
		limit = len(p.alerts)
	}
	// Return most recent
	start := len(p.alerts) - limit
	out := make([]models.Alert, limit)
	copy(out, p.alerts[start:])
	// Reverse so newest first
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func (p *Processor) GetCorrelationPoints(limit int) []models.CorrelationPoint {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var points []models.CorrelationPoint
	for zone, tWindow := range p.trafficWindow {
		// Find corresponding AQ station
		aqStation := "aq_" + zone
		aqWindow, ok := p.aqWindow[aqStation]
		if !ok {
			continue
		}
		n := len(tWindow)
		if len(aqWindow) < n {
			n = len(aqWindow)
		}
		if n > limit {
			n = limit
		}
		for i := 0; i < n; i++ {
			t := tWindow[len(tWindow)-n+i]
			a := aqWindow[len(aqWindow)-n+i]
			wCondition := ""
			wWind := 0.0
			if wWindow, ok := p.weatherWindow["wth_center"]; ok && len(wWindow) > 0 {
				wCondition = wWindow[len(wWindow)-1].Condition
				wWind = wWindow[len(wWindow)-1].WindSpeedMS
			}
			points = append(points, models.CorrelationPoint{
				Zone:            zone,
				Timestamp:       t.Timestamp,
				CongestionIndex: t.CongestionIndex,
				AQI:             a.AQI,
				PM25:            a.PM25,
				WindSpeedMS:     wWind,
				Condition:       wCondition,
			})
		}
	}
	return points
}

func (p *Processor) ComputeCorrelationStats() models.CorrelationStats {
	points := p.GetCorrelationPoints(500)
	if len(points) < 5 {
		return models.CorrelationStats{ComputedAt: time.Now()}
	}

	// Pearson correlation between congestion and AQI
	trafficVsAQI := pearson(
		mapSlice(points, func(pt models.CorrelationPoint) float64 { return pt.CongestionIndex }),
		mapSlice(points, func(pt models.CorrelationPoint) float64 { return float64(pt.AQI) }),
	)
	trafficVsPM25 := pearson(
		mapSlice(points, func(pt models.CorrelationPoint) float64 { return pt.CongestionIndex }),
		mapSlice(points, func(pt models.CorrelationPoint) float64 { return pt.PM25 }),
	)
	windVsAQI := pearson(
		mapSlice(points, func(pt models.CorrelationPoint) float64 { return pt.WindSpeedMS }),
		mapSlice(points, func(pt models.CorrelationPoint) float64 { return float64(pt.AQI) }),
	)

	return models.CorrelationStats{
		TrafficVsAQI:   math.Round(trafficVsAQI*1000) / 1000,
		TrafficVsPM25:  math.Round(trafficVsPM25*1000) / 1000,
		WindVsAQI:      math.Round(windVsAQI*1000) / 1000,
		ComputedAt:     time.Now(),
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────

func (p *Processor) getZoneTrafficForStation(station string) (models.TrafficReading, bool) {
	// Station names like "aq_downtown" map to zone "downtown"
	for _, r := range p.latestTraffic {
		if r.Zone == station[3:] { // strip "aq_" prefix
			return r, true
		}
	}
	return models.TrafficReading{}, false
}

func pearson(x, y []float64) float64 {
	n := len(x)
	if n != len(y) || n < 2 {
		return 0
	}
	var sumX, sumY, sumXY, sumX2, sumY2 float64
	for i := 0; i < n; i++ {
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
		sumY2 += y[i] * y[i]
	}
	fn := float64(n)
	num := sumXY - (sumX*sumY)/fn
	den := math.Sqrt((sumX2-(sumX*sumX)/fn)*(sumY2-(sumY*sumY)/fn))
	if den == 0 {
		return 0
	}
	return num / den
}

func mapSlice[T any, R any](s []T, f func(T) R) []R {
	result := make([]R, len(s))
	for i, v := range s {
		result[i] = f(v)
	}
	return result
}
