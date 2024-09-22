package client

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var bytesRead = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jetstream_client_bytes_read",
	Help: "The total number of bytes read from the server",
}, []string{"client"})

var eventsRead = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "jetstream_client_events_read",
	Help: "The total number of events read from the server",
}, []string{"client"})
