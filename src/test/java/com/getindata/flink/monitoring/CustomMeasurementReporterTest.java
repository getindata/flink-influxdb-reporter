package com.getindata.flink.monitoring;


import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySortedMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import io.vavr.collection.HashMap;
import io.vavr.collection.TreeMap;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import metrics_influxdb.api.measurements.KeyValueMetricMeasurementTransformer;
import metrics_influxdb.measurements.Measure;
import metrics_influxdb.measurements.Sender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class CustomMeasurementReporterTest {

  @Mock
  private Sender sender;

  @Mock
  private MetricRegistry registry;

  @Mock
  private MetricFilter metricFilter;

  @Mock
  private Clock clock;

  @BeforeEach
  private void init() {
    MockitoAnnotations.initMocks(this);
    Mockito.when(clock.getTime()).thenReturn(1L);
  }

  @Test
  void testTagsCanBeWrittenAsValues() {
    final CustomMeasurementReporter reporter = new CustomMeasurementReporter(
        registry,
        metricFilter,
        TimeUnit.MILLISECONDS,
        TimeUnit.SECONDS,
        clock,
        emptyMap(),
        new KeyValueMetricMeasurementTransformer(),
        sender,
        newArrayList("tm_id")
    );

    SortedMap<String, Counter> counters = TreeMap.of(
        "operator.1.tm_id.12333.name", new Counter()
    ).toJavaMap();

    reporter.report(
        emptySortedMap(),
        counters,
        emptySortedMap(),
        emptySortedMap(),
        emptySortedMap()
    );

    final ArgumentCaptor<Measure> measureCaptor = ArgumentCaptor.forClass(Measure.class);
    Mockito.verify(sender).send(measureCaptor.capture());

    final Measure measure = measureCaptor.getValue();
    assertEquals("name", measure.getName());
    assertEquals(1L, measure.getTimestamp());
    assertEquals(HashMap.of("tm_id", "\"12333\"", "count", "0i").toJavaMap(), measure.getValues());
    assertEquals(HashMap.of("operator", "1").toJavaMap(), measure.getTags());
  }

  @Test
  void testBaseTagsCanBeNull() {
    final CustomMeasurementReporter reporter = new CustomMeasurementReporter(
        registry,
        metricFilter,
        TimeUnit.MILLISECONDS,
        TimeUnit.SECONDS,
        clock,
        null,
        new KeyValueMetricMeasurementTransformer(),
        sender,
        newArrayList("tm_id")
    );

    SortedMap<String, Counter> counters = TreeMap.of(
        "operator.1.tm_id.12333.name", new Counter()
    ).toJavaMap();

    reporter.report(
        emptySortedMap(),
        counters,
        emptySortedMap(),
        emptySortedMap(),
        emptySortedMap()
    );

    final ArgumentCaptor<Measure> measureCaptor = ArgumentCaptor.forClass(Measure.class);
    Mockito.verify(sender).send(measureCaptor.capture());

    final Measure measure = measureCaptor.getValue();
    assertEquals("name", measure.getName());
    assertEquals(1L, measure.getTimestamp());
    assertEquals(HashMap.of("tm_id", "\"12333\"", "count", "0i").toJavaMap(), measure.getValues());
    assertEquals(HashMap.of("operator", "1").toJavaMap(), measure.getTags());
  }
}
