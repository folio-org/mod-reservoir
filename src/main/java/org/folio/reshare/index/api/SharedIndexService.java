package org.folio.reshare.index.api;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.reshare.index.storage.Storage;

public class SharedIndexService {

  private static final Logger log = LogManager.getLogger(SharedIndexService.class);
  final Vertx vertx;

  public SharedIndexService(Vertx vertx) {
    this.vertx = vertx;
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
    final JsonObject source = requestJson.getJsonObject("source");
    final JsonObject inventory = requestJson.getJsonObject("inventory");
    MatchKey matchKey = new MatchKey(inventory.getJsonObject("instance"));
    inventory.getJsonObject("instance").put("matchKey", matchKey.getKey());

    storage(routingCtxt)
        .upsertBibRecord(localIdentifier, libraryId, matchKey.getKey(), source, inventory)
        .onComplete(
          bibRecord -> {
            if (bibRecord.succeeded()) {
              goodResponse(routingCtxt);
            } else {
              errorResponse(routingCtxt, requestJson, bibRecord.cause().getMessage());
            }
          });
  }

  private Storage storage(RoutingContext routingContext) {
    String tenant = routingContext.request().getHeader(XOkapiHeaders.TENANT);
    return new Storage(vertx, tenant);
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
