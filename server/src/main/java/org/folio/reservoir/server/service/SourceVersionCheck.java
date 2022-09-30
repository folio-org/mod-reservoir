package org.folio.reservoir.server.service;

import io.vertx.core.Future;
import java.util.HashMap;
import java.util.Map;
import org.folio.reservoir.server.storage.SourceStorage;
import org.folio.reservoir.server.storage.Storage;
import org.folio.reservoir.util.SourceId;

public class SourceVersionCheck {

  Map<String, Boolean> sourceCache = new HashMap<>();

  /**
   * Check if source active version is same as update version.
   * @param storage tenant storage
   * @param sourceId source identifier
   * @param updateVersion version that is used for ingest/OAI client harvest
   * @return async boolean result
   */
  public Future<Boolean> check(Storage storage, SourceId sourceId, int updateVersion) {
    if (sourceCache.containsKey(sourceId.toString())) {
      return Future.succeededFuture(sourceCache.get(sourceId.toString()));
    }
    return SourceStorage.get(storage, sourceId.toString())
        .map(source -> {
          boolean match = source == null // if no configuration for source at all
              || source.getVersion() == null // if no version for source
              || source.getVersion() == updateVersion; // set: must match
          sourceCache.put(sourceId.toString(), match);
          return match;
        });
  }
}
