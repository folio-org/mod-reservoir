package org.folio.reservoir.server.service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import org.folio.okapi.common.HttpResponse;
import org.folio.reservoir.server.data.Source;
import org.folio.reservoir.server.storage.SourceStorage;
import org.folio.reservoir.server.storage.Storage;

public class SourceService {

  private SourceService() {}

  /**
   * Create source.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> postSource(RoutingContext ctx) {
    return SourceStorage.insert(new Storage(ctx), ctx.body().asPojo(Source.class))
        .onSuccess(res -> ctx.response().setStatusCode(204).end());
  }

  /**
   * Get sources.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> getSources(RoutingContext ctx) {
    return SourceStorage.list(new Storage(ctx), ctx);
  }

  /**
   * Get source.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> getSource(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = params.pathParameter("id").getString();
    return SourceStorage.get(storage, id)
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
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = params.pathParameter("id").getString();
    return SourceStorage.delete(storage, id)
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
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = params.pathParameter("id").getString();
    Source source = ctx.body().asPojo(Source.class);
    if (!id.equals(source.getId())) {
      HttpResponse.responseError(ctx, 400, "Source id mismatch");
      return Future.succeededFuture();
    }
    return SourceStorage.update(storage, source)
        .onSuccess(res -> ctx.response().setStatusCode(204).end());
  }
}
