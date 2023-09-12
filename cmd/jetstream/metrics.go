package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var subscribersConnected = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "jetstream_subscribers_connected",
	Help: "The number of subscribers connected to the Jetstream server",
}, []string{"format", "ip_address"})

var eventsEmitted = promauto.NewCounter(prometheus.CounterOpts{
	Name: "jetstream_events_emitted_total",
	Help: "The total number of events emitted by the Jetstream server",
})

var bytesEmitted = promauto.NewCounter(prometheus.CounterOpts{
	Name: "jetstream_bytes_emitted_total",
	Help: "The total number of bytes emitted by the Jetstream server",
})

var eventsDelivered = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jetstream_events_delivered_total",
	Help: "The total number of events delivered by the Jetstream server",
}, []string{"format", "ip_address"})

var bytesDelivered = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jetstream_bytes_delivered_total",
	Help: "The total number of bytes delivered by the Jetstream server",
}, []string{"format", "ip_address"})
