package org.folio.metastorage.matchkey;

import io.vertx.core.json.JsonObject;
import java.util.Collection;
import org.folio.metastorage.matchkey.impl.MatchKeyJsonPath;

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

  /**
   * Generate match keys.
   * @param payload payload with marc and inventory XSLT result
   * @param keys resulting keys (unmodified if no keys were generated).
   */
  void getKeys(JsonObject payload, Collection<String> keys);
}
