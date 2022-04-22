package org.folio.shared.index.api;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import java.util.UUID;
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

  Future<Void> putGlobalRecords(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    return storage.updateGlobalRecords(ctx.getBodyAsJson()).onSuccess(res -> {
      JsonArray ar = new JsonArray();
      // global ids and match keys here ...
      HttpResponse.responseJson(ctx, 200).end(ar.encode());
    });
  }

  static PgCqlQuery createPgCqlQuery() {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(
        new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    return pgCqlQuery;
  }

  static PgCqlQuery getPqCqlQueryForRecords() {
    PgCqlQuery pgCqlQuery = createPgCqlQuery();
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.UUID));
    pgCqlQuery.addField(
        new PgCqlField("id", "globalId", PgCqlField.Type.UUID));
    pgCqlQuery.addField(
        new PgCqlField("local_id", "localId", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("source_id", "sourceId", PgCqlField.Type.UUID));
    return pgCqlQuery;
  }

  Future<Void> deleteGlobalRecords(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = getPqCqlQueryForRecords();
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String query = Util.getQueryParameter(params);
    if (query == null) {
      failHandler(400, ctx, "Must specify query for delete records");
      return Future.succeededFuture();
    }
    pgCqlQuery.parse(query);
    Storage storage = new Storage(ctx);
    return storage.deleteGlobalRecords(pgCqlQuery.getWhereClause())
        .onSuccess(x -> ctx.response().setStatusCode(204).end());
  }

  Future<Void> getGlobalRecords(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = getPqCqlQueryForRecords();
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(Util.getQueryParameter(params));
    Storage storage = new Storage(ctx);
    return storage.getGlobalRecords(ctx, pgCqlQuery.getWhereClause(),
        pgCqlQuery.getOrderByClause());
  }

  Future<Void> getGlobalRecord(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("globalId"));
    Storage storage = new Storage(ctx);
    return storage.getGlobalRecord(id)
        .onSuccess(res -> {
          if (res == null) {
            HttpResponse.responseError(ctx, 404, id);
            return;
          }
          HttpResponse.responseJson(ctx, 200).end(res.encode());
        })
        .mapEmpty();
  }

  void matchKeyNotFound(RoutingContext ctx, String id) {
    HttpResponse.responseError(ctx, 404, "MatchKey " + id + " not found");
  }

  Future<Void> getClusters(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = createPgCqlQuery();
    pgCqlQuery.addField(
        new PgCqlField("match_value", "matchValue", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("cluster_records.cluster_id", "clusterId", PgCqlField.Type.UUID));

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(Util.getQueryParameter(params));
    String matchKeyId = Util.getParameterString(params.queryParameter("matchkeyid"));
    Storage storage = new Storage(ctx);
    return storage.selectMatchKeyConfig(matchKeyId).compose(conf -> {
      if (conf == null) {
        matchKeyNotFound(ctx, matchKeyId);
        return Future.succeededFuture();
      }
      return storage.getClusters(ctx, matchKeyId,
          pgCqlQuery.getWhereClause(), pgCqlQuery.getOrderByClause());
    });
  }

  Future<Void> getCluster(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("clusterId"));
    Storage storage = new Storage(ctx);
    return storage.getClusterById(UUID.fromString(id))
        .onSuccess(res -> {
          if (res.getJsonArray("records").isEmpty()) {
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
    String id = Util.getParameterString(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.selectMatchKeyConfig(id)
        .onSuccess(res -> {
          if (res == null) {
            matchKeyNotFound(ctx, id);
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
            matchKeyNotFound(ctx, id);
            return;
          }
          ctx.response().setStatusCode(204).end();
        })
        .mapEmpty();
  }

  Future<Void> deleteConfigMatchKey(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.deleteMatchKeyConfig(id)
        .onSuccess(res -> {
          if (Boolean.FALSE.equals(res)) {
            matchKeyNotFound(ctx, id);
            return;
          }
          ctx.response().setStatusCode(204).end();
        })
        .mapEmpty();
  }

  Future<Void> getConfigMatchKeys(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = createPgCqlQuery();
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("method", PgCqlField.Type.TEXT));

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(Util.getQueryParameter(params));

    Storage storage = new Storage(ctx);
    return storage.getMatchKeyConfigs(ctx, pgCqlQuery.getWhereClause(),
        pgCqlQuery.getOrderByClause());
  }

  Future<Void> initializeMatchKey(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.initializeMatchKey(id)
        .onSuccess(res -> {
          if (res == null) {
            matchKeyNotFound(ctx, id);
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
    HttpServerResponse response = ctx.response();
    if (response.headWritten()) {
      if (!response.ended()) {
        ctx.response().end();
      }
      return;
    }
    response.setStatusCode(statusCode);
    response.putHeader("Content-Type", "text/plain");
    response.end(msg != null ? msg : "Failure");
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
          add(routerBuilder, "putGlobalRecords", this::putGlobalRecords);
          add(routerBuilder, "getGlobalRecords", this::getGlobalRecords);
          add(routerBuilder, "deleteGlobalRecords", this::deleteGlobalRecords);
          add(routerBuilder, "getGlobalRecord", this::getGlobalRecord);
          add(routerBuilder, "postConfigMatchKey", this::postConfigMatchKey);
          add(routerBuilder, "getConfigMatchKey", this::getConfigMatchKey);
          add(routerBuilder, "putConfigMatchKey", this::putConfigMatchKey);
          add(routerBuilder, "deleteConfigMatchKey", this::deleteConfigMatchKey);
          add(routerBuilder, "getConfigMatchKeys", this::getConfigMatchKeys);
          add(routerBuilder, "initializeMatchKey", this::initializeMatchKey);
          add(routerBuilder, "getClusters", this::getClusters);
          add(routerBuilder, "getCluster", this::getCluster);
          add(routerBuilder, "oaiService", OaiService::get);
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
