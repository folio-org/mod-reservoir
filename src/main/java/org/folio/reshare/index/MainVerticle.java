package org.folio.reshare.index;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.Config;
import org.folio.okapi.common.ModuleVersionReporter;
import org.folio.reshare.index.api.SharedIndexService;
import org.folio.tenantlib.postgres.TenantPgPool;

public class MainVerticle extends AbstractVerticle {
  final Logger log = LogManager.getLogger(MainVerticle.class);

  @Override
  public void start(Promise<Void> promise) {
    TenantPgPool.setModule("mod-reshare-index");
    ModuleVersionReporter m = new ModuleVersionReporter("org.folio/mod-reshare-index");
    log.info("Starting {} {} {}", m.getModule(), m.getVersion(), m.getCommitId());

    final int port = Integer.parseInt(
        Config.getSysConf("http.port", "port", "8081", config()));

    WebClient webClient = WebClient.create(vertx);

    Router router = Router.router(vertx);

    SharedIndexService sharedIndexService = new SharedIndexService(vertx);

    router.put("/*").handler(BodyHandler.create());
    router.delete("/*").handler(BodyHandler.create());
    router.get("/reshare-index/shared-titles").handler(sharedIndexService::handleGetSharedTitles);
    router.put("/reshare-index/shared-titles")
            .handler(sharedIndexService::handlePutSharedTitle);
    router.get("/admin/health").handler(ctx -> {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().end("OK");
    });
    router.post("/_/tenant").handler(ctx -> {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().end("OK");
    });

    RouterBuilder.create(vertx, "openapi/tenant-2.0.yaml").onComplete(routerBuilderAsyncResult -> {
      Router tenantRouter = routerBuilderAsyncResult.result().createRouter();
      router.mountSubRouter("/", tenantRouter);
      Future<Void> future = Future.succeededFuture();
      future = future.compose(x -> {
        HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
        return vertx.createHttpServer(so)
                .requestHandler(router)
                .listen(port).mapEmpty();
      });
      future.onComplete(promise);
    });

  }
}
