// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The main package for the Prometheus server executable.
package main

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/timescale/prometheus-postgresql-adapter/postgresql"

	"github.com/prometheus/prometheus/storage/remote"
)

type config struct {
	remoteTimeout      time.Duration
	listenAddr         string
	telemetryPath      string
	pgPrometheusConfig pgprometheus.Config
}

var (
	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	sentSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sent_samples_total",
			Help: "Total number of processed samples sent to remote storage.",
		},
		[]string{"remote"},
	)
	failedSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "failed_samples_total",
			Help: "Total number of processed samples which failed on send to remote storage.",
		},
		[]string{"remote"},
	)
	sentBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sent_batch_duration_seconds",
			Help:    "Duration of sample batch send calls to the remote storage.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"remote"},
	)
	durationOpts = prometheus.HistogramOpts{
		Name:    "request_duration_seconds",
		Help:    "A histogram of latencies for requests.",
		Buckets: []float64{0.1, 0.2, 0.5, 1, 2.5, 5},
	}
	readVec  *prometheus.HistogramVec
	writeVec *prometheus.HistogramVec
)

type bucket []float64

func (b *bucket) String() string {
	return fmt.Sprint(*b)
}

func (b *bucket) Set(value string) error {
	if len(*b) > 0 {
		return errors.New("bucket flag already set")
	}
	for _, dt := range strings.Split(value, ",") {
		duration, err := strconv.ParseFloat(dt, 64)
		if err != nil {
			return err
		}
		*b = append(*b, duration)
	}
	return nil
}

var bucketFlag bucket

func init() {
	flag.Var(&bucketFlag, "buckets", "Comma-separated list of intervals to bucketize response times")
	if len(bucketFlag) > 0 {
		durationOpts.Buckets = bucketFlag
	}
	durationOpts.ConstLabels = prometheus.Labels{"handler": "read"}
	readVec = prometheus.NewHistogramVec(durationOpts, []string{})
	durationOpts.ConstLabels = prometheus.Labels{"handler": "write"}
	writeVec = prometheus.NewHistogramVec(durationOpts, []string{})

	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(sentSamples)
	prometheus.MustRegister(failedSamples)
	prometheus.MustRegister(sentBatchDuration)
	prometheus.MustRegister(readVec)
	prometheus.MustRegister(writeVec)
}

func main() {
	cfg := parseFlags()
	http.Handle(cfg.telemetryPath, prometheus.Handler())

	writers, readers := buildClients(cfg)
	serve(cfg.listenAddr, writers, readers)
}

func parseFlags() *config {

	cfg := &config{}

	pgprometheus.ParseFlags(&cfg.pgPrometheusConfig)

	flag.DurationVar(&cfg.remoteTimeout, "send-timeout", 30*time.Second,
		"The timeout to use when sending samples to the remote storage.",
	)
	flag.StringVar(&cfg.listenAddr, "web.listen-address", ":9201", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.telemetryPath, "web.telemetry-path", "/metrics", "Address to listen on for web endpoints.")

	flag.Parse()

	return cfg
}

type writer interface {
	Write(samples model.Samples) error
	Name() string
}

type reader interface {
	Read(req *remote.ReadRequest) (*remote.ReadResponse, error)
	Name() string
	HealthCheck() error
}

func buildClients(cfg *config) ([]writer, []reader) {
	var writers []writer
	var readers []reader

	pgClient := pgprometheus.NewClient(&cfg.pgPrometheusConfig)
	writers = append(writers, pgClient)
	readers = append(readers, pgClient)

	return writers, readers
}

func serve(addr string, writers []writer, readers []reader) error {
	write := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req remote.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		samples := protoToSamples(&req)
		receivedSamples.Add(float64(len(samples)))

		var wg sync.WaitGroup
		for _, w := range writers {
			wg.Add(1)
			go func(rw writer) {
				sendSamples(rw, samples)
				wg.Done()
			}(w)
		}
		wg.Wait()
	})
	http.HandleFunc("/write", promhttp.InstrumentHandlerDuration(writeVec, write))

	read := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req remote.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// TODO: Support reading from more than one reader and merging the results.
		if len(readers) != 1 {
			http.Error(w, fmt.Sprintf("expected exactly one reader, found %d readers", len(readers)), http.StatusInternalServerError)
			return
		}
		reader := readers[0]

		var resp *remote.ReadResponse
		resp, err = reader.Read(&req)
		if err != nil {
			log.With("query", req).With("storage", reader.Name()).With("err", err).Warnf("Error executing query")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	http.HandleFunc("/read", promhttp.InstrumentHandlerDuration(readVec, read))

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// Only supports one reader at this point
		reader := readers[0]
		err := reader.HealthCheck()

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "0")
	})

	log.Info("Listening on ", addr)

	err := http.ListenAndServe(addr, nil)

	if err != nil {
		log.Error("Listen failure ", err)
	}
	return err
}

func protoToSamples(req *remote.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.TimestampMs),
			})
		}
	}
	return samples
}

func sendSamples(w writer, samples model.Samples) {
	begin := time.Now()
	err := w.Write(samples)
	duration := time.Since(begin).Seconds()
	if err != nil {
		log.With("num_samples", len(samples)).With("storage", w.Name()).With("err", err).Warnf("Error sending samples to remote storage")
		failedSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
	}
	sentSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
	sentBatchDuration.WithLabelValues(w.Name()).Observe(duration)
}
