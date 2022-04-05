package org.folio.shared.index.matchkey.impl;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.List;
import org.folio.shared.index.matchkey.MatchKeyException;
import org.folio.shared.index.matchkey.MatchKeyMethod;

public class MatchKeyJsonPath implements MatchKeyMethod {

  JsonPath jsonPathMarc;
  JsonPath jsonPathInventory;

  @Override
  public void configure(JsonObject configuration) {
    String expr = configuration.getString("marc");
    if (expr != null) {
      jsonPathMarc = JsonPath.compile(expr);
      return;
    }
    expr = configuration.getString("inventory");
    if (expr != null) {
      jsonPathInventory = JsonPath.compile(expr);
      return;
    }
    throw new MatchKeyException("jsonpath: either \"marc\" or \"inventory\" must be given");
  }

  @Override
  public List<String> getKeys(JsonObject marcPayload, JsonObject inventoryPayload) {
    JsonPath p = jsonPathMarc != null ? jsonPathMarc : jsonPathInventory;
    JsonObject d = jsonPathMarc != null ? marcPayload : inventoryPayload;
    if (p == null) {
      throw new MatchKeyException("Not configured");
    }
    ReadContext ctx = JsonPath.parse(d.encode());
    try {
      Object o = ctx.read(p);
      if (o instanceof String) {
        return List.of((String) o);
      } else if (o instanceof List) {
        for (Object m : (List) o) {
          if (!(m instanceof String)) {
            return Collections.emptyList();
          }
        }
        return (List<String>) o;
      } else {
        return Collections.emptyList();
      }
    } catch (PathNotFoundException e) {
      return Collections.emptyList();
    }
  }
}
