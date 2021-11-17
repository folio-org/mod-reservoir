package org.folio.reshare.index.api;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.reshare.index.storage.Storage;

public class SharedIndexService {

  private static final Logger log = LogManager.getLogger(SharedIndexService.class);
  final Storage storage;

  public SharedIndexService(Vertx vertx) {
    storage = new Storage(vertx);
  }

  /**
   * Doc.
   *
   */
  public void handleGetSharedTitles(RoutingContext routingCtx) {
    routingCtx.response().setStatusCode(200);
    routingCtx.response().putHeader("Content-Type", "text/plain");
    routingCtx.end("To be implemented");
  }


  /**
   * Doc.
   *
   */
  public void handlePutSharedTitle(RoutingContext routingCtxt) {
    JsonObject requestJson = routingCtxt.getBodyAsJson();

    final String localIdentifier = requestJson.getString("localIdentifier");
    final String libraryId = requestJson.getString("libraryId");
    final String matchKey = requestJson.getString("matchKey");
    final JsonObject source = requestJson.getJsonObject("source");
    final JsonObject inventory = requestJson.getJsonObject("inventory");

    storage.upsertBibRecord(localIdentifier, libraryId, matchKey, source, inventory)
        .onComplete(
          bibRecord -> {
            if (bibRecord.succeeded()) {
              goodResponse(routingCtxt);
            } else {
              errorResponse(routingCtxt, requestJson, bibRecord.cause().getMessage());
            }
          });
  }

  private void errorResponse(RoutingContext routingCtxt, JsonObject inputJson, String message) {
    routingCtxt.response().setStatusCode(500);
    routingCtxt.response().putHeader("Content-Type", "text/plain");
    routingCtxt.end("Error creating or updating entry in shared index: "
            + inputJson.encodePrettily() + ": " + message);
  }

  private void goodResponse(RoutingContext routingCtxt) {
    routingCtxt.response().setStatusCode(204).end();
  }
}
