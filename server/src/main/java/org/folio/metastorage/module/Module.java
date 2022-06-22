package org.folio.metastorage.module;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public interface Module {
  
  Future<Void> initialize(Vertx vertx, JsonObject config);

  Future<JsonObject> execute(JsonObject input);

  Future<Void> terminate();
    
}
