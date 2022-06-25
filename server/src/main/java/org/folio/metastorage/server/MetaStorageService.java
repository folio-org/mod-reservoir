package org.folio.metastorage.server;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
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
import org.folio.metastorage.matchkey.MatchKeyMethodFactory;
import org.folio.metastorage.module.ModuleCache;
import org.folio.metastorage.server.entity.CodeModuleEntity;
import org.folio.metastorage.util.LargeJsonReadStream;
import org.folio.okapi.common.HttpResponse;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;
import org.folio.tlib.postgres.PgCqlField;
import org.folio.tlib.postgres.PgCqlQuery;
import org.folio.tlib.util.TenantUtil;

public class MetaStorageService implements RouterCreator, TenantInitHooks {

  private static final Logger log = LogManager.getLogger(MetaStorageService.class);
  final Vertx vertx;
  private static final String ENTITY_ID_NOT_FOUND_PATTERN = "%s %s not found";
  private static final String MODULE_LABEL = "Module";

  public MetaStorageService(Vertx vertx) {
    this.vertx = vertx;
  }

  Future<Void> putGlobalRecords(RoutingContext ctx) {
    try {
      Storage storage = new Storage(ctx);
      HttpServerRequest request = ctx.request();
      request.pause();
      return storage.updateGlobalRecords(ctx.vertx(), new LargeJsonReadStream(request))
          .onSuccess(res -> {
            JsonArray ar = new JsonArray();
            // global ids and match keys here ...
            HttpResponse.responseJson(ctx, 200).end(ar.encode());
          });
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  Future<Void> deleteCodeModule(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.deleteCodeModuleEntity(id)
        .onSuccess(res -> {
          if (Boolean.FALSE.equals(res)) {
            HttpResponse.responseError(ctx, 404,
                String.format(ENTITY_ID_NOT_FOUND_PATTERN, MODULE_LABEL, id));
            return;
          }
          ctx.response().setStatusCode(204).end();
        }).mapEmpty();
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
        new PgCqlField("source_id", "sourceId", PgCqlField.Type.TEXT));
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
    if (MatchKeyMethodFactory.get(method) == null) {
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
    return storage.initializeMatchKey(ctx.vertx(), id)
        .onSuccess(res -> {
          if (res == null) {
            matchKeyNotFound(ctx, id);
            return;
          }
          HttpResponse.responseJson(ctx, 200).end(res.encode());
        })
        .mapEmpty();
  }

  Future<Void> statsMatchKey(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.selectMatchKeyConfig(id)
        .compose(conf -> {
          if (conf == null) {
            matchKeyNotFound(ctx, id);
            return Future.succeededFuture();
          }
          return storage.statsMatchKey(id)
              .onSuccess(res -> HttpResponse.responseJson(ctx, 200).end(res.encode()))
              .mapEmpty();
        });
  }

  //start modules, move to anothe class

  Future<Void> postCodeModule(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    CodeModuleEntity e = new CodeModuleEntity.CodeModuleBuilder(ctx.getBodyAsJson()).build();

    return ModuleCache.getInstance().lookup(ctx.vertx(), TenantUtil.tenant(ctx), e.asJson())
        .compose(module -> storage.insertCodeModuleEntity(e).onSuccess(res ->
            HttpResponse.responseJson(ctx, 201)
                .putHeader("Location", ctx.request().absoluteURI() + "/" + e.getId())
                .end(e.asJson().encode())
        ));
  }

  Future<Void> getCodeModule(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    Storage storage = new Storage(ctx);
    return storage.selectCodeModuleEntity(id)
        .onSuccess(e -> {
          if (e == null) {
            HttpResponse.responseError(ctx, 404,
                String.format(ENTITY_ID_NOT_FOUND_PATTERN, MODULE_LABEL, id));
            return;
          }
          HttpResponse.responseJson(ctx, 200).end(e.asJson().encode());
        })
        .mapEmpty();
  }

  Future<Void> putCodeModule(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    CodeModuleEntity e = new CodeModuleEntity.CodeModuleBuilder(ctx.getBodyAsJson()).build();
    return ModuleCache.getInstance().lookup(ctx.vertx(), TenantUtil.tenant(ctx), e.asJson())
        .compose(module -> storage.updateCodeModuleEntity(e)
            .onSuccess(res -> {
              if (Boolean.FALSE.equals(res)) {
                HttpResponse.responseError(ctx, 404,
                    String.format(ENTITY_ID_NOT_FOUND_PATTERN, MODULE_LABEL, e.getId()));
                return;
              }
              ctx.response().setStatusCode(204).end();
            }))
        .mapEmpty();
  }

  Future<Void> getCodeModules(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = createPgCqlQuery();
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("function", PgCqlField.Type.TEXT));

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(Util.getQueryParameter(params));

    Storage storage = new Storage(ctx);
    return storage.selectCodeModuleEntities(ctx, pgCqlQuery.getWhereClause(),
        pgCqlQuery.getOrderByClause());
  }


  //end modules

  //oai config

  Future<Void> getOaiConfig(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    return storage.selectOaiConfig()
        .onSuccess(res -> {
          if (res == null) {
            HttpResponse.responseError(ctx, 404, "OAI config not found");
            return;
          }
          HttpResponse.responseJson(ctx, 200).end(res.encode());
        })
        .mapEmpty();
  }

  Future<Void> putOaiConfig(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    JsonObject request = ctx.getBodyAsJson();
    return storage.updateOaiConfig(request)
        .onSuccess(res -> {
          if (Boolean.FALSE.equals(res)) {
            HttpResponse.responseError(ctx, 400, "OAI config not updated");
            return;
          }
          ctx.response().setStatusCode(204).end();
        })
        .mapEmpty();
  }

  Future<Void> deleteOaiConfig(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    return storage.deleteOaiConfig()
        .onSuccess(res -> ctx.response().setStatusCode(204).end());
  }

  //end oai config

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
        }).failureHandler(MetaStorageService::failHandler);
  }

  @Override
  public Future<Router> createRouter(Vertx vertx) {
    OaiPmhClientService oaiPmhClient = new OaiPmhClientService(vertx);
    return RouterBuilder.create(vertx, "openapi/meta-storage-1.0.yaml")
        .map(routerBuilder -> {
          add(routerBuilder, "getGlobalRecords", this::getGlobalRecords);
          add(routerBuilder, "deleteGlobalRecords", this::deleteGlobalRecords);
          add(routerBuilder, "getGlobalRecord", this::getGlobalRecord);
          add(routerBuilder, "postConfigMatchKey", this::postConfigMatchKey);
          add(routerBuilder, "getConfigMatchKey", this::getConfigMatchKey);
          add(routerBuilder, "putConfigMatchKey", this::putConfigMatchKey);
          add(routerBuilder, "deleteConfigMatchKey", this::deleteConfigMatchKey);
          add(routerBuilder, "getConfigMatchKeys", this::getConfigMatchKeys);
          add(routerBuilder, "initializeMatchKey", this::initializeMatchKey);
          add(routerBuilder, "statsMatchKey", this::statsMatchKey);
          add(routerBuilder, "getClusters", this::getClusters);
          add(routerBuilder, "getCluster", this::getCluster);
          add(routerBuilder, "oaiService", OaiService::get);
          add(routerBuilder, "postCodeModule", this::postCodeModule);
          add(routerBuilder, "getCodeModule", this::getCodeModule);
          add(routerBuilder, "putCodeModule", this::putCodeModule);
          add(routerBuilder, "deleteCodeModule", this::deleteCodeModule);
          add(routerBuilder, "getCodeModules", this::getCodeModules);
          add(routerBuilder, "getOaiConfig", this::getOaiConfig);
          add(routerBuilder, "putOaiConfig", this::putOaiConfig);
          add(routerBuilder, "deleteOaiConfig", this::deleteOaiConfig);
          add(routerBuilder, "postOaiPmhClient", oaiPmhClient::post);
          add(routerBuilder, "getOaiPmhClient", oaiPmhClient::get);
          add(routerBuilder, "putOaiPmhClient", oaiPmhClient::put);
          add(routerBuilder, "deleteOaiPmhClient", oaiPmhClient::delete);
          add(routerBuilder, "getCollectionOaiPmhClient", oaiPmhClient::getCollection);
          add(routerBuilder, "startOaiPmhClient", oaiPmhClient::start);
          add(routerBuilder, "stopOaiPmhClient", oaiPmhClient::stop);
          add(routerBuilder, "statusOaiPmhClient", oaiPmhClient::status);
          Router router = Router.router(vertx);
          // this endpoint is streaming and we handle it without OpenAPI and validation
          router.put("/meta-storage/records").handler(ctx ->
              putGlobalRecords(ctx).onFailure(cause -> failHandler(400, ctx, cause)));
          router.mountSubRouter("/", routerBuilder.createRouter());
          return router;
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
