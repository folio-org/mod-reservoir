package org.folio.reservoir.util.readstream;

import io.vertx.core.buffer.Buffer;

public class XmlFixerMapper implements Mapper<Buffer, Buffer> {

  private static final String REPLACEMENT_CHAR = "&#xFFFD;";

  private boolean ended;

  Buffer result;

  Buffer pending = null;

  private int numberOfFixes = 0;


  private void incomplete(Buffer input, int i, int back) {
    if (ended) {
      result.appendBuffer(input, back, i - back);
      pending = null;
    } else {
      pending = Buffer.buffer();
      pending.appendBuffer(input, back, i - back);
    }
  }

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
          incomplete(input, i + 1, back);
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
            incomplete(input, j, back);
            return;
          }
          try {
            int v;
            if (input.getByte(i + 2) == 'x') {
              v = Integer.parseInt(input.getString(i + 3, j), 16);
            } else {
              v = Integer.parseInt(input.getString(i + 2, j));
            }
            if (v < 32 && v != 9 && v != 10 && v != 13) {
              result.appendBuffer(input, back, i - back);
              i = j;
              back = i + 1;
              result.appendString(REPLACEMENT_CHAR);
              numberOfFixes++;
            }
          } catch (NumberFormatException e) {
            // ignored; Data will be passed as is
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
