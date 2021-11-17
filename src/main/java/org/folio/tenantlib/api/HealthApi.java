package org.folio.tenantlib.api;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import org.folio.tenantlib.RouterCreator;

public class HealthApi implements RouterCreator {
  @Override
  public Future<Router> createRouter(Vertx vertx, WebClient webClient) {
    Router router = Router.router(vertx);
    router.route(HttpMethod.GET, "/admin/health").handler(ctx -> {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().end("OK");
    });
    return Future.succeededFuture(router);
  }
}
