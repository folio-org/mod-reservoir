package org.folio.reservoir.module;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.Collection;

public interface Module {

  Future<Void> initialize(Vertx vertx, JsonObject config);

  Future<JsonObject> execute(String function, JsonObject input);
  
  Future<JsonObject> execute(JsonObject input);

  Collection<String> executeAsCollection(String function, JsonObject input);
  
  Collection<String> executeAsCollection(JsonObject input);

  Future<Void> terminate();

}
