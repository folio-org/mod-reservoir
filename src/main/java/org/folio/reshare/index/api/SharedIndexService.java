package org.folio.reshare.index.api;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameter;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.reshare.index.storage.Storage;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;
import org.folio.tlib.postgres.PgCqlField;
import org.folio.tlib.postgres.PgCqlQuery;
import org.folio.tlib.util.TenantUtil;

public class SharedIndexService implements RouterCreator, TenantInitHooks {

  private static final Logger log = LogManager.getLogger(SharedIndexService.class);
  final Vertx vertx;

  public SharedIndexService(Vertx vertx) {
    this.vertx = vertx;
  }

  private Storage storage(RoutingContext routingContext) {
    return new Storage(vertx, TenantUtil.tenant(routingContext));
  }

  Future<Void> putSharedTitle(RoutingContext ctx) {
    JsonObject requestJson = ctx.getBodyAsJson();

    final String localIdentifier = requestJson.getString("localIdentifier");
    final String libraryId = requestJson.getString("libraryId");
    final JsonObject source = requestJson.getJsonObject("source");
    final JsonObject inventory = requestJson.getJsonObject("inventory");
    MatchKey matchKey = new MatchKey(inventory.getJsonObject("instance"));
    inventory.getJsonObject("instance").put("matchKey", matchKey.getKey());


    return storage(ctx)
        .upsertBibRecord(localIdentifier, libraryId, source, inventory)
        .onSuccess(bibRecord -> ctx.response().setStatusCode(204).end());
  }

  static String stringOrNull(RequestParameter requestParameter) {
    return requestParameter == null ? null : requestParameter.getString();
  }

  Future<Void> getSharedTitles(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(
        new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.UUID));
    pgCqlQuery.addField(
        new PgCqlField("local_identifier", "localIdentifier", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("library_id", "libraryId", PgCqlField.Type.UUID));

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(stringOrNull(params.queryParameter("query")));

    Storage storage = storage(ctx);
    return storage.getTitles(ctx, pgCqlQuery.getWhereClause(), pgCqlQuery.getOrderByClause());
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
  public Future<Router> createRouter(Vertx vertx) {
    return RouterBuilder.create(vertx, "openapi/shared-index-1.0.yaml")
        .map(routerBuilder -> {
          add(routerBuilder, "getSharedTitles", ctx -> getSharedTitles(ctx));
          add(routerBuilder, "putSharedTitle", ctx -> putSharedTitle(ctx));
          return routerBuilder.createRouter();
        });
  }

  @Override
  public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    if (!tenantAttributes.containsKey("module_to")) {
      return Future.succeededFuture(); // doing nothing for disable
    }
    Storage storage = new Storage(vertx, tenant);
    return storage.init();
  }
}
