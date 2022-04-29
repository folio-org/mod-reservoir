package org.folio.metastorage.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class AsyncCodec {

  /**
   * Compress buffer with gzip.
   * @param vertx context
   * @param in input buffer
   * @return future compressed buffer
   */
  public static Future<Buffer> compress(Vertx vertx, Buffer in) {
    return vertx.executeBlocking(fut -> {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
        gzos.write(in.getBytes());
        gzos.close();
        fut.complete(Buffer.buffer(baos.toByteArray()));
      } catch (IOException ioe) {
        fut.fail(ioe);
      }
    }, false);
  }
  
  /**
   * Decompress gzip compressed buffer.
   * @param vertx context
   * @param in input buffer
   * @return future uncompressed buffer
   */
  public static Future<Buffer> decompress(Vertx vertx, Buffer in) {
    return vertx.executeBlocking(fut -> {
      byte[] buffer = new byte[1024];
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ByteArrayInputStream bis = new ByteArrayInputStream(in.getBytes());
          GZIPInputStream gzis = new GZIPInputStream(bis)) {
        int bytesRead;
        while ((bytesRead = gzis.read(buffer)) > 0) {
          baos.write(buffer, 0, bytesRead);
        }
        gzis.close();
        baos.close();
        fut.complete(Buffer.buffer(baos.toByteArray()));
      } catch (IOException ioe) {
        fut.fail(ioe.getMessage());
      }
    }, false);
  }
  
}
