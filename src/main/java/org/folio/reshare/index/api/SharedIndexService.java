package org.folio.reshare.index.api;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.openapi.RouterBuilder;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.reshare.index.storage.Storage;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;

public class SharedIndexService implements RouterCreator, TenantInitHooks {

  private static final Logger log = LogManager.getLogger(SharedIndexService.class);
  final Vertx vertx;

  public SharedIndexService(Vertx vertx) {
    this.vertx = vertx;
  }

  private Storage storage(RoutingContext routingContext) {
    String tenant = routingContext.request().getHeader(XOkapiHeaders.TENANT);
    return new Storage(vertx, tenant);
  }

  Future<Void> putSharedTitle(Vertx vertx, RoutingContext ctx) {
    JsonObject requestJson = ctx.getBodyAsJson();

    final String localIdentifier = requestJson.getString("localIdentifier");
    final String libraryId = requestJson.getString("libraryId");
    final JsonObject source = requestJson.getJsonObject("source");
    final JsonObject inventory = requestJson.getJsonObject("inventory");
    MatchKey matchKey = new MatchKey(inventory.getJsonObject("instance"));
    inventory.getJsonObject("instance").put("matchKey", matchKey.getKey());

    return storage(ctx)
        .upsertBibRecord(localIdentifier, libraryId, matchKey.getKey(), source, inventory)
        .onSuccess(bibRecord -> ctx.response().setStatusCode(204).end());
  }

  Future<Void> getSharedTitles(Vertx vertx, RoutingContext ctx) {
    throw new RuntimeException("getSharedTitles: not implemented");
  }

  static void failHandler(RoutingContext ctx) {
    Throwable t = ctx.failure();
    // both semantic errors and syntax errors are from same pile ... Choosing 400 over 422.
    int statusCode = t.getClass().getName().startsWith("io.vertx.ext.web.validation") ? 400 : 500;
    failHandler(statusCode, ctx, t.getMessage());
  }

  static void failHandler(int statusCode, RoutingContext ctx, Throwable e) {
    log.error(e.getMessage(), e);
    failHandler(statusCode, ctx, e.getMessage());
  }

  static void failHandler(int statusCode, RoutingContext ctx, String msg) {
    ctx.response().setStatusCode(statusCode);
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().end(msg != null ? msg : "Failure");
  }

  private void add(RouterBuilder routerBuilder, String operationId,
      Function<RoutingContext, Future<Void>> function) {
    routerBuilder
        .operation(operationId)
        .handler(ctx -> {
          try {
            function.apply(ctx)
                .onFailure(cause -> failHandler(400, ctx, cause));
          } catch (Throwable t) {
            failHandler(400, ctx, t);
          }
        }).failureHandler(SharedIndexService::failHandler);
  }

  @Override
  public Future<Router> createRouter(Vertx vertx, WebClient webClient) {
    return RouterBuilder.create(vertx, "openapi/reshare-index-1.0.yaml")
        .map(routerBuilder -> {
          add(routerBuilder, "getSharedTitles", ctx -> getSharedTitles(vertx, ctx));
          add(routerBuilder, "putSharedTitle", ctx -> putSharedTitle(vertx, ctx));
          return routerBuilder.createRouter();
        });
  }
}
