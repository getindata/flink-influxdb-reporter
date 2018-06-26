package com.getindata.flink.monitoring;


import static java.util.Collections.emptyMap;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Singular;
import lombok.val;
import metrics_influxdb.HttpInfluxdbProtocol;
import metrics_influxdb.InfluxdbProtocol;
import metrics_influxdb.UdpInfluxdbProtocol;
import metrics_influxdb.api.measurements.MetricMeasurementTransformer;
import metrics_influxdb.measurements.HttpInlinerSender;
import metrics_influxdb.measurements.Measure;
import metrics_influxdb.measurements.Sender;
import metrics_influxdb.measurements.UdpInlinerSender;
import org.apache.flink.annotation.VisibleForTesting;

class CustomMeasurementReporter extends ScheduledReporter {

  private final Sender sender;
  private final Clock clock;
  private final List<String> tagsAsValue;
  private Map<String, String> baseTags;
  private MetricMeasurementTransformer transformer;

  /**
   * Constructs Measurement reporter that allows publishing some tags as values.
   */
  @Builder
  public CustomMeasurementReporter(
      MetricRegistry registry,
      MetricFilter filter,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      Clock clock,
      Map<String, String> baseTags,
      MetricMeasurementTransformer transformer,
      InfluxdbProtocol protocol,
      @Singular("tagAsValue") List<String> tagsAsValue) {
    this(registry, filter, rateUnit, durationUnit, clock, baseTags, transformer,
        buildSender(Optional.ofNullable(protocol).orElseGet(HttpInfluxdbProtocol::new)),
        tagsAsValue);
  }

  @VisibleForTesting
  CustomMeasurementReporter(
      MetricRegistry registry,
      MetricFilter filter,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      Clock clock,
      Map<String, String> baseTags,
      MetricMeasurementTransformer transformer,
      Sender sender,
      @Singular List<String> tagsAsValue) {
    super(registry, "measurement-reporter",
        Optional.ofNullable(filter).orElse(MetricFilter.ALL),
        Optional.ofNullable(rateUnit).orElse(TimeUnit.SECONDS),
        Optional.ofNullable(durationUnit).orElse(TimeUnit.MILLISECONDS));
    this.baseTags = Optional.ofNullable(baseTags).orElse(emptyMap());
    this.sender = sender;
    this.clock = Optional.ofNullable(clock).orElseGet(Clock::defaultClock);
    this.transformer = Optional.ofNullable(transformer).orElse(MetricMeasurementTransformer.NOOP);
    this.tagsAsValue = Optional.ofNullable(tagsAsValue).orElse(Collections.emptyList());
  }

  private static Sender buildSender(InfluxdbProtocol protocol) {
    if (protocol instanceof HttpInfluxdbProtocol) {
      return new HttpInlinerSender((HttpInfluxdbProtocol) protocol);
      // TODO allow registration of transformers
      // TODO evaluate need of prefix (vs tags)
    } else if (protocol instanceof UdpInfluxdbProtocol) {
      return new UdpInlinerSender((UdpInfluxdbProtocol) protocol);
    } else {
      throw new IllegalStateException("unsupported protocol: " + protocol);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void report(SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {

    final long timestamp = clock.getTime();

    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      sender.send(fromGauge(entry.getKey(), entry.getValue(), timestamp));
    }

    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      sender.send(fromCounter(entry.getKey(), entry.getValue(), timestamp));
    }

    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      sender.send(fromHistogram(entry.getKey(), entry.getValue(), timestamp));
    }

    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      sender.send(fromMeter(entry.getKey(), entry.getValue(), timestamp));
    }

    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      sender.send(fromTimer(entry.getKey(), entry.getValue(), timestamp));
    }

    sender.flush();
  }

  private Measure fromTimer(String metricName, Timer t, long timestamp) {
    Snapshot snapshot = t.getSnapshot();

    Measure measure = new Measure(transformer.measurementName(metricName))
        .timestamp(timestamp)
        .addValue("count", snapshot.size())
        .addValue("min", convertDuration(snapshot.getMin()))
        .addValue("max", convertDuration(snapshot.getMax()))
        .addValue("mean", convertDuration(snapshot.getMean()))
        .addValue("std-dev", convertDuration(snapshot.getStdDev()))
        .addValue("50-percentile", convertDuration(snapshot.getMedian()))
        .addValue("75-percentile", convertDuration(snapshot.get75thPercentile()))
        .addValue("95-percentile", convertDuration(snapshot.get95thPercentile()))
        .addValue("99-percentile", convertDuration(snapshot.get99thPercentile()))
        .addValue("999-percentile", convertDuration(snapshot.get999thPercentile()))
        .addValue("one-minute", convertRate(t.getOneMinuteRate()))
        .addValue("five-minute", convertRate(t.getFiveMinuteRate()))
        .addValue("fifteen-minute", convertRate(t.getFifteenMinuteRate()))
        .addValue("mean-minute", convertRate(t.getMeanRate()))
        .addValue("run-count", t.getCount());

    return addTags(metricName, measure);
  }

  private Measure fromMeter(String metricName, Meter mt, long timestamp) {
    Measure measure = new Measure(transformer.measurementName(metricName))
        .timestamp(timestamp)
        .addValue("count", mt.getCount())
        .addValue("one-minute", convertRate(mt.getOneMinuteRate()))
        .addValue("five-minute", convertRate(mt.getFiveMinuteRate()))
        .addValue("fifteen-minute", convertRate(mt.getFifteenMinuteRate()))
        .addValue("mean-minute", convertRate(mt.getMeanRate()));
    return addTags(metricName, measure);
  }

  private Measure fromHistogram(String metricName, Histogram h, long timestamp) {
    Snapshot snapshot = h.getSnapshot();

    Measure measure = new Measure(transformer.measurementName(metricName))
        .timestamp(timestamp)
        .addValue("count", snapshot.size())
        .addValue("min", snapshot.getMin())
        .addValue("max", snapshot.getMax())
        .addValue("mean", snapshot.getMean())
        .addValue("std-dev", snapshot.getStdDev())
        .addValue("50-percentile", snapshot.getMedian())
        .addValue("75-percentile", snapshot.get75thPercentile())
        .addValue("95-percentile", snapshot.get95thPercentile())
        .addValue("99-percentile", snapshot.get99thPercentile())
        .addValue("999-percentile", snapshot.get999thPercentile())
        .addValue("run-count", h.getCount());
    return addTags(metricName, measure);
  }

  private Measure fromCounter(String metricName, Counter c, long timestamp) {
    Measure measure = new Measure(transformer.measurementName(metricName))
        .timestamp(timestamp)
        .addValue("count", c.getCount());

    return addTags(metricName, measure);
  }

  private Measure addTags(String metricName, Measure measure) {

    val tags2 = io.vavr.collection.HashMap.ofAll(transformer.tags(metricName))
        .span(t -> tagsAsValue.contains(t._1()));

    Map<String, String> tagsAsTags = new HashMap<>(baseTags);
    tagsAsTags.putAll(tags2._2.toJavaMap());
    val tagsAsValues = tags2._1;

    measure.addTag(tagsAsTags);
    tagsAsValues.forEach(t -> measure.addValue(t._1, t._2));
    return measure;
  }

  @SuppressWarnings("rawtypes")
  private Measure fromGauge(String metricName, Gauge g, long timestamp) {

    Measure measure = new Measure(transformer.measurementName(metricName))
        .timestamp(timestamp);
    Object o = g.getValue();

    if (o == null) {
      // skip null values
      return null;
    }
    if (o instanceof Long || o instanceof Integer) {
      long value = ((Number) o).longValue();
      measure.addValue("value", value);
    } else if (o instanceof Double) {
      Double d = (Double) o;
      if (d.isInfinite() || d.isNaN()) {
        // skip Infinite & NaN
        return null;
      }
      measure.addValue("value", d);
    } else if (o instanceof Float) {
      Float f = (Float) o;
      if (f.isInfinite() || f.isNaN()) {
        // skip Infinite & NaN
        return null;
      }
      measure.addValue("value", f);
    } else {
      String value = "" + o;
      measure.addValue("value", value);
    }

    return addTags(metricName, measure);
  }
}
