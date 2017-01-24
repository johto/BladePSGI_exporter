package main

import (
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// The list of backend states we track.  This needs to be entered by the user
// because we don't know what any of them mean.
var trackBackendStates = "_"

// statsMetrics
const (
	SM_BLADEPSGI_SCRAPE_TIME int = iota
	SM_BLADEPSGI_START_TIME
	SM_BACKENDS_TOTAL
	SM_BACKENDS_PER_STATUS_TOTAL
	SM_REQUESTS_TOTAL
	SM_USER_SEM_VALUE
	SM_USER_ATOMIC_VALUE
)

type BPECollector struct {
	elog *log.Logger
	resolvedStatsSocketPath *net.UnixAddr

	constMetrics []prometheus.Metric
	statsMetrics map[int]*prometheus.Desc
}

func newBPECollector(elog *log.Logger, statsSocketPath string) *BPECollector {
	targetAddr, err := net.ResolveUnixAddr("unix", statsSocketPath)
	if err != nil {
		elog.Fatalf("invalid stats socket path: %s", err)
	}

	c := &BPECollector{
		elog: elog,
		resolvedStatsSocketPath: targetAddr,
	}

	c.constMetrics = []prometheus.Metric{
		prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				"bladepsgi_exporter_start_time",
				"The time at which the BladePSGI exporter was started",
				nil,
				nil,
			),
			prometheus.GaugeValue,
			float64(time.Now().Unix()),
		),
	}
	c.statsMetrics = map[int]*prometheus.Desc{
		SM_BLADEPSGI_SCRAPE_TIME: prometheus.NewDesc(
			"bladepsgi_exporter_scrape_time_seconds",
			"The time it took to scrape BladePSGI",
			nil,
			nil,
		),
		SM_BLADEPSGI_START_TIME: prometheus.NewDesc(
			"bladepsgi_start_time",
			"The time at which the BladePSGI instance was started",
			nil,
			nil,
		),
		SM_BACKENDS_TOTAL: prometheus.NewDesc(
			"bladepsgi_backends_total",
			"The total number of application backends running",
			nil,
			nil,
		),
		SM_BACKENDS_PER_STATUS_TOTAL: prometheus.NewDesc(
			"bladepsgi_backends_per_status_total",
			"The total number of application backends per status",
			[]string{"status"},
			nil,
		),
		SM_REQUESTS_TOTAL: prometheus.NewDesc(
			"bladepsgi_requests_total",
			"The total number of requests accepted",
			nil,
			nil,
		),
		SM_USER_SEM_VALUE: prometheus.NewDesc(
			"bladepsgi_user_sem_value",
			"The current value of a semaphore created in the loader",
			[]string{"name"},
			nil,
		),
		SM_USER_ATOMIC_VALUE: prometheus.NewDesc(
			"bladepsgi_user_atomic_value",
			"The current value of an atomic integer created in the loader",
			[]string{"name"},
			nil,
		),
	}

	return c
}

type BPEScrapeData struct {
	// The time at which the BladePSGI instance started
	StartTime time.Time
	NumBackends int
	BackendStatuses map[byte]int
	RequestsTotal int64

	UserSemaphoreValues map[string]int64
	UserAtomicValues map[string]int64
}

func (c *BPECollector) scrape() (scrapeData BPEScrapeData) {
	conn, err := net.DialUnix("unix", nil, c.resolvedStatsSocketPath)
	if err != nil {
		c.elog.Fatalf("could not connect to BladePSGI: %s", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	data, err := ioutil.ReadAll(conn)
	if err != nil {
		c.elog.Fatalf("could not data from BladePSGI: %s", err)
	}

	datums := bytes.Split(data, []byte{'\n'})
	// an empty newline separates the internal data from user-created objects
	separator := -1
	for i, d := range datums {
		if len(d) == 0 {
			separator = i
			break
		}
	}
	if separator == -1 {
		c.elog.Fatalf("invalid response from BladePSGI: could not find the separating empty line")
	}
	if len(datums[len(datums) - 1]) != 0 {
		c.elog.Fatalf("invalid response from BladePSGI: unexpected data after the last line")
	}
	internalDatums := datums[:separator]
	userDatums := datums[separator + 1:len(datums) - 1]

	if len(internalDatums) < 3 {
		c.elog.Fatalf("invalid response from BladePSGI: internal data section too short")
	}

	startTime, err := strconv.ParseUint(string(internalDatums[0]), 10, 63)
	if err != nil {
		c.elog.Fatalf("invalid response from BladePSGI: invalid start time")
	}
	scrapeData.StartTime = time.Unix(int64(startTime), 0)
	scrapeData.NumBackends = len(internalDatums[1])

	scrapeData.BackendStatuses = make(map[byte]int)
	for _, st := range []byte(trackBackendStates) {
		scrapeData.BackendStatuses[st] = 0
	}
	for _, st := range internalDatums[1] {
		_, ok := scrapeData.BackendStatuses[st]
		if ok {
			scrapeData.BackendStatuses[st] += 1
		}
	}

	requestsTotal, err := strconv.ParseInt(string(internalDatums[2]), 10, 64)
	if err != nil {
		c.elog.Fatalf("invalid response from BladePSGI: invalid total number of requests")
	}
	scrapeData.RequestsTotal = requestsTotal

	scrapeData.UserSemaphoreValues = make(map[string]int64)
	scrapeData.UserAtomicValues = make(map[string]int64)
	for _, datum := range userDatums {
		datum := string(datum)
		if strings.HasPrefix(datum, "sem ") {
			nameEnd := strings.IndexByte(datum, ':')
			if nameEnd == -1 {
				c.elog.Fatalf("could not find a colon on user semaphore line %s", datum)
			}
			semName := datum[4:nameEnd]
			semValue, err := strconv.ParseInt(strings.TrimSpace(datum[nameEnd+1:]), 10, 64)
			if err != nil {
				c.elog.Fatalf("could not parse the value from user semaphore line %s", datum)
			}
			scrapeData.UserSemaphoreValues[semName] = semValue
		} else if strings.HasPrefix(datum, "atomic ") {
			nameEnd := strings.IndexByte(datum, ':')
			if nameEnd == -1 {
				c.elog.Fatalf("could not find a colon on user semaphore line %s", datum)
			}
			varName := datum[7:nameEnd]
			varValue, err := strconv.ParseInt(strings.TrimSpace(datum[nameEnd+1:]), 10, 64)
			if err != nil {
				c.elog.Fatalf("could not parse the value from user semaphore line %s", datum)
			}
			scrapeData.UserAtomicValues[varName] = varValue
		} else {
			// N.B: Forwards compatibility
		}
	}

	return scrapeData
}

func (c *BPECollector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.constMetrics {
		ch <- m.Desc()
	}
	for _, desc := range c.statsMetrics {
		ch <- desc
	}
}

func (c *BPECollector) Collect(ch chan<- prometheus.Metric) {
	scrapeStart := time.Now()
	scrapeData := c.scrape()
	scrapeTime := time.Now().Sub(scrapeStart)

	for _, m := range c.constMetrics {
		ch <- m
	}
	ch <- prometheus.MustNewConstMetric(c.statsMetrics[SM_BLADEPSGI_SCRAPE_TIME], prometheus.GaugeValue, float64(scrapeTime.Seconds()))
	ch <- prometheus.MustNewConstMetric(c.statsMetrics[SM_BLADEPSGI_START_TIME], prometheus.GaugeValue, float64(scrapeData.StartTime.Unix()))
	ch <- prometheus.MustNewConstMetric(c.statsMetrics[SM_BACKENDS_TOTAL], prometheus.GaugeValue, float64(scrapeData.NumBackends))
	for st, num := range scrapeData.BackendStatuses {
		ch <- prometheus.MustNewConstMetric(c.statsMetrics[SM_BACKENDS_PER_STATUS_TOTAL], prometheus.GaugeValue, float64(num), string([]byte{st}))
	}
	ch <- prometheus.MustNewConstMetric(c.statsMetrics[SM_REQUESTS_TOTAL], prometheus.GaugeValue, float64(scrapeData.RequestsTotal))

	for sname, val := range scrapeData.UserSemaphoreValues {
		ch <- prometheus.MustNewConstMetric(c.statsMetrics[SM_USER_SEM_VALUE], prometheus.UntypedValue, float64(val), sname)
	}

	for aname, val := range scrapeData.UserAtomicValues {
		ch <- prometheus.MustNewConstMetric(c.statsMetrics[SM_USER_ATOMIC_VALUE], prometheus.UntypedValue, float64(val), aname)
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, `Usage:
  %s [--help] STATS_SOCKET_PATH
`, os.Args[0])
}

func main() {
	var statsSocketPath string

	if len(os.Args) != 2 {
		printUsage(os.Stderr)
		os.Exit(1)
	}
	if os.Args[1] == "--help" || os.Args[1] == "-h" {
		printUsage(os.Stdout)
		os.Exit(0)
	} else {
		statsSocketPath = os.Args[1]
	}

	elog := log.New(os.Stderr, "", log.LstdFlags)
	elog.Printf("BladePSGI exporter starting up")

	collector := newBPECollector(elog, statsSocketPath)

	registry := prometheus.NewPedanticRegistry()
	err := registry.Register(collector)
	if err != nil {
		elog.Fatalf("ERROR:  %s", err)
	}
	httpHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		ErrorLog: elog,
	})
	http.Handle("/metrics", httpHandler)
	elog.Fatal(http.ListenAndServe(":9223", nil))
}
