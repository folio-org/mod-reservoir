package org.folio.reservoir.server;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UploadDocument {

  private static final Logger log = LogManager.getLogger(UploadDocument.class);

  /**
   * Create upload document handler.
   * @param contentType content type for multipart file
   * @param filename filename for file
   * @param sourceId source id (library identifier)
   * @param sourceVersion source version
   * @param localIdPath json-path for how to extract local identifier
   */
  public UploadDocument(String contentType, String filename,
      String sourceId, String sourceVersion, String localIdPath) {
    log.info("contentType = {} filename = {}", contentType, filename);
    log.info("sourceId = {} versionVersion = {} localIdPath = {}",
        sourceId, sourceVersion, localIdPath);
    switch (contentType) {
      case "application/octet-stream":
      case "application/marc":
        // TODO add more content-types
        break;
      default:
        throw new IllegalArgumentException("Unsupported content-type: " + contentType);
    }
  }

  Future<Void> handler(Buffer buf) {
    // TODO
    return Future.succeededFuture();
  }

  Future<Void> endHandler() {
    // TODO
    return Future.succeededFuture();
  }
}
