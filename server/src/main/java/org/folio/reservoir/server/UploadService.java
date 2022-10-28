package org.folio.reservoir.server;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.Pump;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.HttpResponse;

public class UploadService {

  private static final Logger log = LogManager.getLogger(UploadService.class);

  /**
   * non-OpenAPI upload records service handler.
   *
   * <p>Do not produce http response in case of failures.
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> uploadRecords(RoutingContext ctx) {
    HttpServerRequest request = ctx.request();
    String sourceId = request.getParam("sourceId");
    String sourceVersion = request.getParam("sourceVersion");
    String localIdPath = request.getParam("localIdPath");
    Promise<Void> promise = Promise.promise();
    request.uploadHandler(upload -> {
      try {
        UploadDocument uploadDocument = new UploadDocument(upload.contentType(), upload.filename(),
            sourceId, sourceVersion, localIdPath);
        Pump pump = Pump.pump(upload, uploadDocument);
        pump.start();
      } catch (Exception e) {
        promise.tryFail(e);
      }
    });
    request.endHandler(e -> {
      if (promise.tryComplete()) {
        JsonObject res = new JsonObject();
        HttpResponse.responseJson(ctx, 200).end(res.encode());
      }
    });
    request.setExpectMultipart(true);
    return promise.future();
  }

}
