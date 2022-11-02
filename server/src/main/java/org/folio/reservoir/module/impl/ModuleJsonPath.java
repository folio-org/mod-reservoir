package org.folio.reservoir.module.impl;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.folio.reservoir.module.Module;

public class ModuleJsonPath implements Module {

  JsonPath jsonPath;

  @Override
  public Future<Void> initialize(Vertx vertx, JsonObject config) {
    String script = config.getString("script");
    if (script == null) {
      return Future.failedFuture("module config must include 'script'");
    }
    jsonPath = JsonPath.compile(script);
    return Future.succeededFuture();
  }

  @Override
  public Future<JsonObject> execute(JsonObject input) {
    return execute(null, input);
  }

  @Override
  public Future<JsonObject> execute(String function, JsonObject input) {
    throw new UnsupportedOperationException("only executeAsCollection supported for type=jsonpath");
  }
  
  @Override
  public Collection<String> executeAsCollection(JsonObject input) {
    return executeAsCollection(null, input);
  }

  @Override
  public Collection<String> executeAsCollection(String function, JsonObject input) {
    if (jsonPath == null) {
      throw new IllegalStateException("uninitialized");
    }
    ReadContext ctx = JsonPath.parse(input.encode());
    Collection<String> keys = new HashSet<>();
    try {
      Object o = ctx.read(jsonPath);
      if (o instanceof String string) {
        keys.add(string);
      } else if (o instanceof List<?> list) {
        for (Object m : list) {
          if (!(m instanceof String)) {
            return keys;
          }
        }
        keys.addAll((List<String>) o);
      }
    } catch (PathNotFoundException e) {
      //ignore
    }
    return keys;
  }

  @Override
  public Future<Void> terminate() {
    jsonPath = null;
    return Future.succeededFuture();
  }
  
}
