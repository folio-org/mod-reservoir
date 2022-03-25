package org.folio.shared.index.matchkey;

import io.vertx.core.json.JsonObject;
import java.util.List;
import org.folio.shared.index.matchkey.impl.MatchKeyJsonPath;

public interface MatchKeyMethod {

  /**
   * Get MatchKeyMethod instance from method.
   * @param method method name
   * @return method or NULL if not found
   */
  static MatchKeyMethod get(String method) {
    if ("jsonpath".equals(method)) {
      return new MatchKeyJsonPath();
    }
    return null;
  }

  void configure(JsonObject configuration);

  List<String> getKeys(JsonObject marcPayload, JsonObject inventoryPayload);
}
