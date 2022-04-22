package org.folio.shared.index.matchkey.impl;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import io.vertx.core.json.JsonObject;
import java.util.Collection;
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
  public void getKeys(JsonObject marcPayload, JsonObject inventoryPayload,
      Collection<String> keys) {
    JsonPath p = jsonPathMarc != null ? jsonPathMarc : jsonPathInventory;
    JsonObject d = jsonPathMarc != null ? marcPayload : inventoryPayload;
    if (p == null) {
      throw new MatchKeyException("Not configured");
    }
    ReadContext ctx = JsonPath.parse(d.encode());
    try {
      Object o = ctx.read(p);
      if (o instanceof String) {
        keys.add((String) o);
      } else if (o instanceof List) {
        for (Object m : (List) o) {
          if (!(m instanceof String)) {
            return;
          }
        }
        keys.addAll((List<String>) o);
      }
    } catch (PathNotFoundException e) {
      // ignored.. no keys added
    }
  }
}
