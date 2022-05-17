package org.folio.metastorage.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.Config;
import org.folio.okapi.common.ModuleVersionReporter;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.api.HealthApi;
import org.folio.tlib.api.Tenant2Api;
import org.folio.tlib.postgres.TenantPgPool;

public class MainVerticle extends AbstractVerticle {
  final Logger log = LogManager.getLogger(MainVerticle.class);

  @Override
  public void start(Promise<Void> promise) {
    TenantPgPool.setModule("mod-meta-storage");
    ModuleVersionReporter m = new ModuleVersionReporter("org.folio/mod-meta-storage-server");
    log.info("Starting {} {} {}", m.getModule(), m.getVersion(), m.getCommitId());

    final int port = Integer.parseInt(
        Config.getSysConf("http.port", "port", "8081", config()));
    log.info("Listening on port {}", port);

    MetaStorageService sharedIndexService = new MetaStorageService(vertx);

    RouterCreator[] routerCreators = {
        sharedIndexService,
        new Tenant2Api(sharedIndexService),
        new HealthApi(),
    };

    vertx.executeBlocking(x -> JavaScriptCheck.check());

    RouterCreator.mountAll(vertx, routerCreators)
        .compose(router -> {
          HttpServerOptions so = new HttpServerOptions()
              .setCompressionSupported(true)
              .setDecompressionSupported(true)
              .setHandle100ContinueAutomatically(true);
          return vertx.createHttpServer(so)
              .requestHandler(router)
              .listen(port).mapEmpty();
        })
        .onComplete(x -> promise.handle(x.mapEmpty()));
  }
}
