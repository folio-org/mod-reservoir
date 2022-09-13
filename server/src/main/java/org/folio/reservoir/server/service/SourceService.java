package org.folio.reservoir.server.service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import org.folio.okapi.common.HttpResponse;
import org.folio.reservoir.server.data.Source;
import org.folio.reservoir.server.storage.SourceStorage;

public class SourceService {

  private SourceService() {}

  /**
   * Create source.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> postSource(RoutingContext ctx) {
    SourceStorage storage = new SourceStorage(ctx);
    return storage.insert(ctx.body().asPojo(Source.class))
        .onSuccess(res -> ctx.response().setStatusCode(204).end());
  }

  /**
   * Get sources.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> getSources(RoutingContext ctx) {
    return new SourceStorage(ctx).list(ctx);
  }

  /**
   * Get source.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> getSource(RoutingContext ctx) {
    SourceStorage storage = new SourceStorage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = params.pathParameter("id").getString();
    return storage.get(id)
        .onSuccess(res -> {
          if (res == null) {
            HttpResponse.responseError(ctx, 404, "Source " + id + " not found");
          } else {
            HttpResponse.responseJson(ctx, 200).end(JsonObject.mapFrom(res).encode());
          }
        })
        .mapEmpty();
  }

  /**
   * Delete source.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> deleteSource(RoutingContext ctx) {
    SourceStorage storage = new SourceStorage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = params.pathParameter("id").getString();
    return storage.delete(id)
        .onSuccess(res -> {
          if (Boolean.FALSE.equals(res)) {
            HttpResponse.responseError(ctx, 404, "Source " + id + " not found");
          } else {
            ctx.response().setStatusCode(204).end();
          }
        })
        .mapEmpty();
  }

  /**
   * Put source.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> putSource(RoutingContext ctx) {
    SourceStorage storage = new SourceStorage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = params.pathParameter("id").getString();
    Source source = ctx.body().asPojo(Source.class);
    if (!id.equals(source.getId())) {
      HttpResponse.responseError(ctx, 400, "Source id mismatch");
      return Future.succeededFuture();
    }
    return storage.update(source)
        .onSuccess(res -> ctx.response().setStatusCode(204).end());
  }
}
