package org.folio.reservoir.server;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.okapi.common.HttpResponse;

public class UploadService {

  /**
   * Upload records service handler.
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> upload(RoutingContext ctx) {
    JsonObject res = new JsonObject();
    // TODO does nothing at the moment
    HttpResponse.responseJson(ctx, 200).end(res.encode());
    return Future.succeededFuture();
  }
}
