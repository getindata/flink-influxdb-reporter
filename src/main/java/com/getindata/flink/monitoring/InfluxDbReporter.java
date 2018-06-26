package com.getindata.flink.monitoring;


import static java.util.Objects.nonNull;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import metrics_influxdb.HttpInfluxdbProtocol;
import metrics_influxdb.api.measurements.KeyValueMetricMeasurementTransformer;
import org.apache.flink.dropwizard.ScheduledDropwizardReporter;
import org.apache.flink.metrics.MetricConfig;

/**
 * InfluxDB Metrics Reporter for Apache Flink.
 *
 * <p>In order to use this add the following configuration to your flink-conf.yaml file:
 *
 * <p><pre>
 * metrics.reporters: influxdb
 * metrics.reporter.influxdb.scheme: http
 * metrics.reporter.influxdb.server: localhost
 * metrics.reporter.influxdb.port: 8086
 * metrics.reporter.influxdb.user: username
 * metrics.reporter.influxdb.password: password
 * metrics.reporter.influxdb.db: cep
 * metrics.reporter.influxdb.class: com.getindata.flink.monitoring.InfluxDbReporter
 * metrics.reporter.influxdb.interval: 60 SECONDS
 * metrics.reporter.influxdb.tagsAsValue: &lt;comma separated list of tags that should be reported
 * as tags&gt;
 * </pre>
 *
 * <p>Please be advised that when using {@link KeyValueMetricMeasurementTransformer}, metrics names
 * must have appropriate structure:
 * <pre>
 * key1.value1.key2.value2.metricName
 * </pre>
 * or
 * <pre>
 * key1.value1.key2.value2.metricNamePart1.metricNamePart2
 * </pre>
 * The metrics format is configured via the following properties:
 * <ul>
 *   <li>metrics.scope.jm</li>
 *   <li>metrics.scope.jm.job</li>
 *   <li>metrics.scope.tm</li>
 *   <li>metrics.scope.tm.job</li>
 *   <li>metrics.scope.task</li>
 *   <li>metrics.scope.operator</li>
 * </ul>
 */
public class InfluxDbReporter extends ScheduledDropwizardReporter {

  @Override
  public ScheduledReporter getReporter(MetricConfig config) {
    String scheme = config.getString("scheme", "http");
    String server = config.getString("server", "localhost");
    Integer port = config.getInteger("port", 8086);
    String user = config.getString("user", null);
    String password = config.getString("password", null);
    String db = config.getString("db", "default");
    final List<String> tagsAsValue = Optional.ofNullable(config.getString("tagsAsValue", null))
        .map(s -> Arrays.asList(s.split(",")))
        .orElse(Collections.emptyList());

    HttpInfluxdbProtocol protocol = nonNull(user) && nonNull(password)
        ? new HttpInfluxdbProtocol(scheme, server, port, user, password, db)
        : new HttpInfluxdbProtocol(scheme, server, port, null, null, db);

    return CustomMeasurementReporter.builder()
        .registry(registry)
        .protocol(protocol)
        .rateUnit(TimeUnit.SECONDS)
        .durationUnit(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .tagsAsValue(tagsAsValue)
        .transformer(new KeyValueMetricMeasurementTransformer())
        .build();
  }

}
