//! Prometheus text format metrics with latency accumulator and rate tracking.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const HISTOGRAM_MAX_SAMPLES: usize = 10_000;

#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub counters: HashMap<String, u64>,
    pub gauges: HashMap<String, f64>,
    pub histograms: HashMap<String, Vec<f64>>,
}

impl MetricsSnapshot {
    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        for (name, value) in &self.counters {
            out.push_str(&format!("# TYPE {} counter\n{} {}\n", name, name, value));
        }
        for (name, value) in &self.gauges {
            out.push_str(&format!("# TYPE {} gauge\n{} {}\n", name, name, value));
        }
        for (name, values) in &self.histograms {
            if values.is_empty() {
                continue;
            }
            let mut sorted = values.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let len = sorted.len();
            let p50 = sorted[len * 50 / 100];
            let p95 = sorted[len * 95 / 100];
            let p99 = sorted.get(len * 99 / 100).copied().unwrap_or(p95);
            let p999 = sorted.get(len * 999 / 1000).copied().unwrap_or(p99);
            out.push_str(&format!("# TYPE {} gauge\n", name));
            out.push_str(&format!("{}{{quantile=\"0.5\"}} {}\n", name, p50));
            out.push_str(&format!("{}{{quantile=\"0.95\"}} {}\n", name, p95));
            out.push_str(&format!("{}{{quantile=\"0.99\"}} {}\n", name, p99));
            out.push_str(&format!("{}{{quantile=\"0.999\"}} {}\n", name, p999));
        }
        out
    }
}

struct HistogramReservoir {
    samples: Vec<f64>,
    count: u64,
}

impl HistogramReservoir {
    fn new() -> Self {
        Self {
            samples: Vec::new(),
            count: 0,
        }
    }

    fn record(&mut self, value: f64) {
        self.count += 1;
        if self.samples.len() < HISTOGRAM_MAX_SAMPLES {
            self.samples.push(value);
        } else {
            let idx = (self.count as usize) % HISTOGRAM_MAX_SAMPLES;
            self.samples[idx] = value;
        }
    }
}

pub struct MetricsCollector {
    counters: Arc<tokio::sync::RwLock<HashMap<String, Arc<AtomicU64>>>>,
    gauges: Arc<tokio::sync::RwLock<HashMap<String, f64>>>,
    histograms: Arc<tokio::sync::RwLock<HashMap<String, HistogramReservoir>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            counters: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            gauges: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            histograms: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub async fn increment_counter(&self, name: &str, value: u64) {
        {
            let counters = self.counters.read().await;
            if let Some(counter) = counters.get(name) {
                counter.fetch_add(value, Ordering::Relaxed);
                return;
            }
        }
        let mut counters = self.counters.write().await;
        let counter = counters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        counter.fetch_add(value, Ordering::Relaxed);
    }

    pub async fn set_gauge(&self, name: &str, value: f64) {
        let mut gauges = self.gauges.write().await;
        gauges.insert(name.to_string(), value);
    }

    pub async fn record_histogram(&self, name: &str, value: f64) {
        let mut histograms = self.histograms.write().await;
        histograms
            .entry(name.to_string())
            .or_insert_with(HistogramReservoir::new)
            .record(value);
    }

    pub async fn snapshot(&self) -> MetricsSnapshot {
        let counters = {
            let c = self.counters.read().await;
            c.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect()
        };
        let gauges = {
            let g = self.gauges.read().await;
            g.clone()
        };
        let histograms = {
            let h = self.histograms.read().await;
            h.iter()
                .map(|(k, v)| (k.clone(), v.samples.clone()))
                .collect()
        };
        MetricsSnapshot {
            counters,
            gauges,
            histograms,
        }
    }

    pub async fn reset(&self) {
        {
            let c = self.counters.write().await;
            for v in c.values() {
                v.store(0, Ordering::Relaxed);
            }
        }
        {
            let mut g = self.gauges.write().await;
            g.clear();
        }
        {
            let mut h = self.histograms.write().await;
            h.clear();
        }
    }
}

/// Thread-safe latency accumulator using sorted Vec for percentile computation.
pub struct LatencyAccumulator {
    inner: std::sync::Mutex<LatAccumInner>,
}

struct LatAccumInner {
    samples: Vec<f64>,
    count: u64,
}

impl LatencyAccumulator {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(LatAccumInner {
                samples: Vec::new(),
                count: 0,
            }),
        }
    }

    pub fn record(&self, d: Duration) {
        let ms = d.as_secs_f64() * 1000.0;
        let mut inner = self.inner.lock().unwrap();
        inner.count += 1;
        if inner.samples.len() < HISTOGRAM_MAX_SAMPLES {
            inner.samples.push(ms);
        } else {
            let idx = (inner.count as usize) % HISTOGRAM_MAX_SAMPLES;
            inner.samples[idx] = ms;
        }
    }

    pub fn record_ms(&self, ms: f64) {
        let mut inner = self.inner.lock().unwrap();
        inner.count += 1;
        if inner.samples.len() < HISTOGRAM_MAX_SAMPLES {
            inner.samples.push(ms);
        } else {
            let idx = (inner.count as usize) % HISTOGRAM_MAX_SAMPLES;
            inner.samples[idx] = ms;
        }
    }

    /// Returns the percentile value in milliseconds.
    pub fn percentile_ms(&self, p: f64) -> f64 {
        let inner = self.inner.lock().unwrap();
        if inner.samples.is_empty() {
            return 0.0;
        }
        let mut sorted = inner.samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        let idx = idx.min(sorted.len() - 1);
        sorted[idx]
    }

    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.samples.clear();
        inner.count = 0;
    }

    pub fn count(&self) -> u64 {
        self.inner.lock().unwrap().count
    }
}

/// 10-second sliding window peak rate tracker.
pub struct PeakRateTracker {
    inner: std::sync::Mutex<PeakRateInner>,
}

struct PeakRateInner {
    buckets: [i64; 10],
    idx: usize,
    peak: f64,
}

impl PeakRateTracker {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(PeakRateInner {
                buckets: [0; 10],
                idx: 0,
                peak: 0.0,
            }),
        }
    }

    pub fn record(&self, count: i64) {
        let mut inner = self.inner.lock().unwrap();
        let idx = inner.idx;
        inner.buckets[idx] += count;
    }

    pub fn advance(&self) {
        let mut inner = self.inner.lock().unwrap();
        let total: i64 = inner.buckets.iter().sum();
        let rate = total as f64 / 10.0;
        if rate > inner.peak {
            inner.peak = rate;
        }
        inner.idx = (inner.idx + 1) % 10;
        let idx = inner.idx;
        inner.buckets[idx] = 0;
    }

    pub fn peak(&self) -> f64 {
        self.inner.lock().unwrap().peak
    }

    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.buckets = [0; 10];
        inner.idx = 0;
        inner.peak = 0.0;
    }
}

/// 30-second sliding window rate calculator.
pub struct SlidingRateWindow {
    inner: std::sync::Mutex<SlidingRateInner>,
}

struct SlidingRateInner {
    buckets: [i64; 30],
    idx: usize,
    total: i64,
    ticks: usize,
}

impl SlidingRateWindow {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(SlidingRateInner {
                buckets: [0; 30],
                idx: 0,
                total: 0,
                ticks: 0,
            }),
        }
    }

    pub fn record(&self, count: i64) {
        let mut inner = self.inner.lock().unwrap();
        let idx = inner.idx;
        inner.buckets[idx] += count;
        inner.total += count;
    }

    pub fn advance(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.ticks += 1;
        inner.idx = (inner.idx + 1) % 30;
        let idx = inner.idx;
        inner.total -= inner.buckets[idx];
        inner.buckets[idx] = 0;
    }

    pub fn rate(&self) -> f64 {
        let inner = self.inner.lock().unwrap();
        let window = inner.ticks.min(30);
        if window == 0 {
            return 0.0;
        }
        inner.total as f64 / window as f64
    }

    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.buckets = [0; 30];
        inner.idx = 0;
        inner.total = 0;
        inner.ticks = 0;
    }
}
