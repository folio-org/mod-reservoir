package org.folio.reservoir.util.readstream;

import io.vertx.core.buffer.Buffer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.zip.GZIPInputStream;

public class GzipDecompressMapper implements Mapper<Buffer, Buffer> {
  private Buffer buffer = Buffer.buffer();

  @Override
  public void push(Buffer item) {
    buffer.appendBuffer(item);
  }

  @Override
  public Buffer poll(boolean ended) {
    if (buffer.length() == 0) {
      return null;
    }
    try {
      InputStream bais = new ByteArrayInputStream(buffer.getBytes());
      GZIPInputStream gis = new GZIPInputStream(bais);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(2 * buffer.length());
      byte[] buf = new byte[1024];
      int len;
      while ((len = gis.read(buf)) != -1) {
        baos.write(buf, 0, len);
      }
      //close resources, does nothing
      baos.close();
      gis.close();
      buffer = Buffer.buffer();
      return Buffer.buffer(baos.toByteArray());
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }
  
}
