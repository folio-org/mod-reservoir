package org.folio.reservoir.server;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;

public class SruService {

  /**
   * SRU handler to get a record by query (id field).
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> get(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    HttpServerResponse response = ctx.response();
    response.setChunked(true);
    response.putHeader("Content-Type", "text/xml");
    response.setStatusCode(200);
    final String query = Util.getParameterString(params.queryParameter("query"));
    if (query == null) {
      // TODO: return explain response
      response.write("Query is required");
      response.end();
      return Future.succeededFuture();
    }
    // TODO: implement record fetch
    response.write("<x/>");
    response.end();
    return Future.succeededFuture();
  }
}
