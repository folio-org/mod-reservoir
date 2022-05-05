package org.folio.metastorage.matchkey.impl;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import io.vertx.core.json.JsonObject;
import java.util.Collection;
import java.util.List;
import org.folio.metastorage.matchkey.MatchKeyException;
import org.folio.metastorage.matchkey.MatchKeyMethod;

public class MatchKeyJsonPath implements MatchKeyMethod {

  JsonPath jsonPath;

  @Override
  public void configure(JsonObject configuration) {
    String expr = configuration.getString("expr");
    if (expr == null) {
      throw new MatchKeyException("jsonpath: expr must be given");
    }
    jsonPath = JsonPath.compile(expr);
  }

  @Override
  public void getKeys(JsonObject payload, Collection<String> keys) {
    if (jsonPath == null) {
      throw new MatchKeyException("Not configured");
    }
    ReadContext ctx = JsonPath.parse(payload.encode());
    try {
      Object o = ctx.read(jsonPath);
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
