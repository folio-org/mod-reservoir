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
import java.util.Arrays;
import java.util.List;
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
  private static final int MATCH_MAX_ITERATIONS = 3;
  final Vertx vertx;

  public SharedIndexService(Vertx vertx) {
    this.vertx = vertx;
  }

  static String getParameterString(RequestParameter parameter) {
    return parameter == null ? null : parameter.getString();
  }

  static int getParameterInteger(RequestParameter parameter, int defaultValue) {
    return parameter == null ? defaultValue : parameter.getInteger();
  }

  Future<Void> putSharedRecords(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    return storage.upsertSharedRecords(ctx.getBodyAsJson()).onSuccess(res -> {
      JsonArray ar = new JsonArray();
      // global ids and match keys here ...
      HttpResponse.responseJson(ctx, 200).end(ar.encode());
    });
  }

  static PgCqlQuery getPqCqlQueryForRecords() {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(
        new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.UUID));
    pgCqlQuery.addField(
        new PgCqlField("local_id", "localId", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("source_id", "sourceId", PgCqlField.Type.UUID));
    return pgCqlQuery;
  }

  static String getQueryParameter(RequestParameters params) {
    return getParameterString(params.queryParameter("query"));
  }

  Future<Void> deleteSharedRecords(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = getPqCqlQueryForRecords();
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String query = getQueryParameter(params);
    if (query == null) {
      failHandler(400, ctx, "Must specify query for delete records");
      return Future.succeededFuture();
    }
    pgCqlQuery.parse(query);
    Storage storage = new Storage(ctx);
    return storage.deleteSharedRecords(pgCqlQuery.getWhereClause())
        .onSuccess(x -> ctx.response().setStatusCode(204).end());
  }

  Future<Void> getSharedRecords(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = getPqCqlQueryForRecords();
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(getQueryParameter(params));
    String m = getParameterString(params.queryParameter("matchkeyid"));
    Storage storage = new Storage(ctx);
    if (m != null) {
      int maxIterations = getParameterInteger(
          params.queryParameter("maxiterations"), MATCH_MAX_ITERATIONS);
      List<String> matchKeyIds = Arrays.asList(m.split(","));
      return storage.getCluster(pgCqlQuery.getWhereClause(), matchKeyIds, maxIterations)
          .map(records -> {
            JsonArray items = new JsonArray();
            records.forEach((k, v) -> items.add(v));
            JsonObject result = new JsonObject();
            result.put("items", items);
            result.put("totalRecords", records.size());
            HttpResponse.responseJson(ctx, 200).end(result.encode());
            return null;
          });
    }
    return storage.getSharedRecords(ctx, pgCqlQuery.getWhereClause(),
        pgCqlQuery.getOrderByClause());
  }

  Future<Void> getSharedRecordGlobalId(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = getParameterString(params.pathParameter("globalId"));
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

  static String getMethod(JsonObject config) {
    String method = config.getString("method");
    if (MatchKeyMethod.get(method) == null) {
      throw new IllegalArgumentException("Non-existing method '" + method + "'");
    }
    return method;
  }

  Future<Void> postConfigMatchKey(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    JsonObject request = ctx.getBodyAsJson();
    String id = request.getString("id");
    String method = getMethod(request);
    String update = request.getString("update", "ingest");
    JsonObject params = request.getJsonObject("params");
    return storage.insertMatchKeyConfig(id, method, params, update).onSuccess(res ->
        HttpResponse.responseJson(ctx, 201)
            .putHeader("Location", ctx.request().absoluteURI() + "/" + id)
            .end(request.encode())
    );
  }

  Future<Void> getConfigMatchKey(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = getParameterString(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.selectMatchKeyConfig(id)
        .onSuccess(res -> {
          if (res == null) {
            HttpResponse.responseError(ctx, 404, "MatchKey " + id + " not found");
            return;
          }
          HttpResponse.responseJson(ctx, 200).end(res.encode());
        })
        .mapEmpty();
  }

  Future<Void> putConfigMatchKey(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    JsonObject request = ctx.getBodyAsJson();
    String id = request.getString("id");
    String method = getMethod(request);
    String update = request.getString("update", "ingest");
    JsonObject params = request.getJsonObject("params");
    return storage.updateMatchKeyConfig(id, method, params, update)
        .onSuccess(res -> {
          if (Boolean.FALSE.equals(res)) {
            HttpResponse.responseError(ctx, 404, "MatchKey " + id + " not found");
            return;
          }
          ctx.response().setStatusCode(204).end();
        })
        .mapEmpty();
  }

  Future<Void> deleteConfigMatchKey(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = getParameterString(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.deleteMatchKeyConfig(id)
        .onSuccess(res -> {
          if (Boolean.FALSE.equals(res)) {
            HttpResponse.responseError(ctx, 404, "MatchKey " + id + " not found");
            return;
          }
          ctx.response().setStatusCode(204).end();
        })
        .mapEmpty();
  }


  Future<Void> getConfigMatchKeys(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(
        new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("method", PgCqlField.Type.TEXT));

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(getQueryParameter(params));

    Storage storage = new Storage(ctx);
    return storage.getMatchKeyConfigs(ctx, pgCqlQuery.getWhereClause(),
        pgCqlQuery.getOrderByClause());
  }

  Future<Void> initializeMatchKey(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = getParameterString(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.initializeMatchKey(id)
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
          add(routerBuilder, "deleteSharedRecords", this::deleteSharedRecords);
          add(routerBuilder, "getSharedRecordGlobalId", this::getSharedRecordGlobalId);
          add(routerBuilder, "postConfigMatchKey", this::postConfigMatchKey);
          add(routerBuilder, "getConfigMatchKey", this::getConfigMatchKey);
          add(routerBuilder, "putConfigMatchKey", this::putConfigMatchKey);
          add(routerBuilder, "deleteConfigMatchKey", this::deleteConfigMatchKey);
          add(routerBuilder, "getConfigMatchKeys", this::getConfigMatchKeys);
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
