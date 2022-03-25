package org.folio.shared.index.api;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
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
import org.folio.okapi.common.HttpResponse;
import org.folio.shared.index.matchkey.MatchKeyMethod;
import org.folio.shared.index.storage.Storage;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;
import org.folio.tlib.postgres.PgCqlField;
import org.folio.tlib.postgres.PgCqlQuery;

public class SharedIndexService implements RouterCreator, TenantInitHooks {

  private static final Logger log = LogManager.getLogger(SharedIndexService.class);
  final Vertx vertx;

  public SharedIndexService(Vertx vertx) {
    this.vertx = vertx;
  }

  static String stringOrNull(RequestParameter requestParameter) {
    return requestParameter == null ? null : requestParameter.getString();
  }

  Future<Void> putSharedRecords(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    return storage.upsertSharedRecords(ctx.getBodyAsJson()).onSuccess(res -> {
      JsonArray ar = new JsonArray();
      // global ids and match keys here ...
      HttpResponse.responseJson(ctx, 200).end(ar.encode());
    });
  }

  Future<Void> getSharedRecords(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(
        new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.UUID));
    pgCqlQuery.addField(
        new PgCqlField("local_id", "localId", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("source_id", "sourceId", PgCqlField.Type.UUID));

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(stringOrNull(params.queryParameter("query")));

    Storage storage = new Storage(ctx);
    return storage.getSharedRecords(ctx, pgCqlQuery.getWhereClause(),
        pgCqlQuery.getOrderByClause());
  }

  Future<Void> getSharedRecordGlobalId(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = stringOrNull(params.pathParameter("globalId"));
    Storage storage = new Storage(ctx);
    return storage.selectSharedRecord(id)
        .onSuccess(res -> {
          if (res == null) {
            HttpResponse.responseError(ctx, 404, id);
            return;
          }
          HttpResponse.responseJson(ctx, 200).end(res.encode());
        })
        .mapEmpty();
  }

  Future<Void> postMatchKey(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    JsonObject request = ctx.getBodyAsJson();
    String id = request.getString("id");
    String method = request.getString("method");
    if (MatchKeyMethod.get(method) == null) {
      return Future.failedFuture("Non-existing method '" + method + "'");
    }
    JsonObject params = request.getJsonObject("params");
    return storage.insertMatchKey(id, method, params).onSuccess(res ->
        HttpResponse.responseJson(ctx, 201)
            .putHeader("Location", ctx.request().absoluteURI() + "/" + id)
            .end(request.encode())
    );
  }

  Future<Void> getMatchKey(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = stringOrNull(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.selectMatchKey(id).onSuccess(res -> {
      if (res == null) {
        HttpResponse.responseError(ctx, 404, "MatchKey " + id + " not found");
        return;
      }
      HttpResponse.responseJson(ctx, 200).end(res.encode());
    }).mapEmpty();
  }

  Future<Void> deleteMatchKey(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = stringOrNull(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.deleteMatchKey(id).onSuccess(res -> {
      if (Boolean.FALSE.equals(res)) {
        HttpResponse.responseError(ctx, 404, "MatchKey " + id + " not found");
        return;
      }
      ctx.response().setStatusCode(204).end();
    }).mapEmpty();
  }


  Future<Void> getMatchKeys(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(
        new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("method", PgCqlField.Type.TEXT));

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(stringOrNull(params.queryParameter("query")));

    Storage storage = new Storage(ctx);
    return storage.getMatchKeys(ctx, pgCqlQuery.getWhereClause(),
        pgCqlQuery.getOrderByClause());
  }

  Future<Void> initializeMatchKey(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = stringOrNull(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.initializeMatchKey(id, 2)
        .onSuccess(res -> {
          if (res == null) {
            HttpResponse.responseError(ctx, 404, "MatchKey " + id + " not found");
            return;
          }
          HttpResponse.responseJson(ctx, 200).end(res.encode());
        })
        .mapEmpty();
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
          } catch (Exception t) {
            failHandler(400, ctx, t);
          }
        }).failureHandler(SharedIndexService::failHandler);
  }

  @Override
  public Future<Router> createRouter(Vertx vertx) {
    return RouterBuilder.create(vertx, "openapi/shared-index-1.0.yaml")
        .map(routerBuilder -> {
          add(routerBuilder, "putSharedRecords", this::putSharedRecords);
          add(routerBuilder, "getSharedRecords", this::getSharedRecords);
          add(routerBuilder, "getSharedRecordGlobalId", this::getSharedRecordGlobalId);
          add(routerBuilder, "postMatchKey", this::postMatchKey);
          add(routerBuilder, "getMatchKey", this::getMatchKey);
          add(routerBuilder, "deleteMatchKey", this::deleteMatchKey);
          add(routerBuilder, "getMatchKeys", this::getMatchKeys);
          add(routerBuilder, "initializeMatchKey", this::initializeMatchKey);
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
