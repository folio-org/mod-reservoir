package org.folio.reservoir.util.readstream;

import io.vertx.core.buffer.Buffer;

public class XmlFixerMapper implements Mapper<Buffer, Buffer> {

  private static final String REPLACEMENT_CHAR = "&#xFFFD;";

  private boolean ended;

  Buffer result;

  Buffer pending = null;

  int numberOfFixes = 0;

  @Override
  public void push(Buffer buffer) {
    Buffer input;
    if (result == null) {
      result = Buffer.buffer();
    }
    // In most cases pending is null; especially for large buffers
    if (pending == null) {
      input = buffer;
    } else {
      pending.appendBuffer(buffer);
      input = pending;
    }
    int back = 0;
    int i;
    for (i = 0; i < input.length(); i++) {
      byte leadingByte = input.getByte(i);
      if (leadingByte < 32 && leadingByte != 9 && leadingByte != 10 && leadingByte != 13) {
        result.appendBuffer(input, back, i - back);
        back = i + 1;
        result.appendString(REPLACEMENT_CHAR);
        numberOfFixes++;
      } else if (leadingByte == '&') {
        if (i == input.length() - 1) {
          if (ended) {
            i++;
            break;
          }
          pending = Buffer.buffer();
          pending.appendBuffer(input, back, i + 1 - back);
          return;
        }
        if (input.getByte(i + 1) == '#') {
          int j = i + 2;
          while (j < input.length()) {
            if (input.getByte(j) == ';') {
              break;
            }
            j++;
          }
          if (j == input.length()) {
            if (ended) {
              i = j;
              break;
            }
            pending = Buffer.buffer();
            pending.appendBuffer(input, back, j - back);
            return;
          }
          int v;
          if (input.getByte(i + 2) == 'x') {
            // &#x....;
            v = Integer.parseInt(input.getString(i + 3, j), 16);
          } else {
            // &#...;
            v = Integer.parseInt(input.getString(i + 2, j));
          }
          if (v < 32 && v != 9 && v != 10 && v != 13) {
            result.appendBuffer(input, back, i - back);
            i = j;
            back = i + 1;
            result.appendString(REPLACEMENT_CHAR);
            numberOfFixes++;
          }
        }
      }
    }
    result.appendBuffer(input, back, i - back);
    pending = null;
  }

  @Override
  public Buffer poll() {
    if (pending != null) {
      return null;
    }
    Buffer ret = result;
    result = null;
    return ret;
  }

  @Override
  public void end() {
    ended = true;
    push(Buffer.buffer());
  }

  public int getNumberOfFixes() {
    return numberOfFixes;
  }
}
