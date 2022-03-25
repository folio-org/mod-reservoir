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
    try {
      if (jsonPathMarc != null) {
        ReadContext ctx = JsonPath.parse(marcPayload.encode());
        return ctx.read(jsonPathMarc, List.class);
      }
      if (jsonPathInventory != null) {
        ReadContext ctx = JsonPath.parse(inventoryPayload.encode());
        return ctx.read(jsonPathInventory, List.class);
      }
    } catch (PathNotFoundException e) {
      return Collections.emptyList();
    }
    throw new MatchKeyException("Not configured");
  }
}
