package org.folio.reservoir.server;

import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxJmxMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.folio.okapi.common.Config;

public class ReservoirLauncher extends Launcher {
  private static final String PROMETHEUS_PORT_DEFAULT = "8082";
  private static final String PROMETHEUS_PORT = "prometheus.port";
  private JsonObject config;

  public static void main(String[] args) {
    new ReservoirLauncher().dispatch(args);
  }

  @Override
  public void afterConfigParsed(JsonObject config) {
    this.config = config;
    super.afterConfigParsed(config);
  }

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    final int port = Integer.parseInt(
        Config.getSysConf(PROMETHEUS_PORT, PROMETHEUS_PORT, PROMETHEUS_PORT_DEFAULT, config));
    options.setMetricsOptions(
      new MicrometerMetricsOptions()
        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true)
            .setStartEmbeddedServer(true)
            .setEmbeddedServerOptions(new HttpServerOptions().setPort(port))
            .setEmbeddedServerEndpoint("/metrics"))
        .setJmxMetricsOptions(new VertxJmxMetricsOptions().setEnabled(true)
            .setStep(5)
            .setDomain("reservoir"))
        .setEnabled(true));
    super.beforeStartingVertx(options);
  }

}
