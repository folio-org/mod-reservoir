package org.folio.reservoir.server;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerFileUpload;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
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

        upload.handler(x -> {
          uploadDocument.handler(x);
          log.info("upload handler got {} bytes", x.length());
        });
        upload.endHandler(e -> {
          MultiMap entries = request.formAttributes();
          entries.forEach((k, v) -> log.info("form {}={}", k, v));
          uploadDocument.endHandler();
          log.info("End upload");
        });
      } catch (Exception e) {
        promise.tryFail(e);
      }
    });
    request.handler(x -> {
      log.info("Got {} bytes", x.length());
    });
    request.handler(x -> log.info("request handler got {}", x.length()));
    request.endHandler(e -> {
      if (promise.tryComplete()) {
        MultiMap entries = request.formAttributes();
        entries.forEach((k, v) -> log.info("form {}={}", k, v));
        JsonObject res = new JsonObject();
        HttpResponse.responseJson(ctx, 200).end(res.encode());
      }
    });
    request.setExpectMultipart(true);
    return promise.future();
  }

}
