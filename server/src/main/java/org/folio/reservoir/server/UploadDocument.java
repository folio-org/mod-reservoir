package org.folio.reservoir.server;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UploadDocument implements WriteStream<Buffer> {
  private static final Logger log = LogManager.getLogger(UploadDocument.class);

  Handler<Void> drainHandler;

  Handler<AsyncResult<Void>> endHandler;

  Handler<Throwable> exceptionHandler;

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

  @Override
  public WriteStream<Buffer> exceptionHandler(@Nullable Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public Future<Void> write(Buffer buffer) {
    return Future.succeededFuture();
  }

  @Override
  public void write(Buffer buffer, Handler<AsyncResult<Void>> handler) {
    write(buffer).onComplete(handler);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    this.endHandler = handler;
    handler.handle(Future.succeededFuture());
  }

  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(int i) {
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return false;
  }

  @Override
  public WriteStream<Buffer> drainHandler(@Nullable Handler<Void> handler) {
    this.drainHandler = handler;
    return this;
  }
}
