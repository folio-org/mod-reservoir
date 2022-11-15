package org.folio.reservoir.util.readstream;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.streams.ReadStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.marc.Record;

public class Marc4jParser extends ReadStreamConverter<Record, Buffer> {
  private Buffer pendingBuffer;

  /**
   * Construct marc4j streaming parser.
   * @param stream ISO2709 encoded byte stream
   */
  public Marc4jParser(ReadStream<Buffer> stream) {
    super(stream);
    pendingBuffer = Buffer.buffer();
  }

  /**
   * Check if we have a complete MARC-record at buffer and return its length.
   * @param buffer memory buffer with presumably ISO2709 data
   * @param offset inspect at this offset and onwards.
   * @return 0 if buffer is incomplete (not a complete MARC record); otherwise
   *     return length of MARC record at buffer at offset.
   * @throws DecodeException if not a MARC record at offset
   */
  static int getNext(Buffer buffer, int offset) {
    // skip up to 4 non-digit bytes (bad data)
    for (int i = 0; i < 4; i++) {
      int remain = buffer.length() - offset;
      if (remain < 5) { // need at least 5 bytes for MARC header
        return 0;
      }
      byte leadByte = buffer.getByte(offset);
      if (leadByte >= '0' && leadByte <= '9') {
        String lead = buffer.getString(offset, offset + 5);
        int length = Integer.parseInt(lead);
        if (length < 24) {
          throw new DecodeException("Bad MARC length");
        }
        return remain >= length ? length : 0;
      }
      offset++;
    }
    throw new DecodeException("Missing MARC header");
  }

  void handlePending() {
    // pick as many MARC buffers as possible and pass only one buffer to Marc4j
    int sz = 0;
    while (demand > 0L) {
      int add = getNext(pendingBuffer, sz);
      if (add == 0) {
        break;
      }
      sz += add;
      if (demand != Long.MAX_VALUE) {
        --demand;
      }
    }
    if (sz > 0) {
      // have one or more MARC record to be parsed by Marc4j
      marc4jpending(sz);
    }
  }

  private void marc4jpending(int sz) {
    try (
        InputStream inputStream = new ByteArrayInputStream(pendingBuffer.getBytes(0, sz))
    ) {
      MarcReader marcReader = new MarcPermissiveStreamReader(inputStream, true, true);
      while (marcReader.hasNext()) {
        Record r = marcReader.next();
        if (eventHandler != null) {
          eventHandler.handle(r);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      pendingBuffer = pendingBuffer.getBuffer(sz, pendingBuffer.length());
    }
  }

  @Override
  public void handle(Buffer buffer) {
    this.pendingBuffer.appendBuffer(buffer);
    checkPending();
  }

}
