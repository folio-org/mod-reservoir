package org.folio.reservoir.util.readstream;

import io.vertx.core.buffer.Buffer;

public class XmlFixerMapper implements Mapper<Buffer, Buffer> {

  private static final String REPLACEMENT_CHAR = "&#xFFFD;";

  private boolean ended;

  Buffer result;

  Buffer pending = null;

  private int numberOfFixes = 0;

  int front;

  int tail;

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
    tail = 0;
    for (front = 0; front < input.length(); front++) {
      byte leadingByte = input.getByte(front);
      if (leadingByte > 0 && leadingByte < 32
          && leadingByte != 9 && leadingByte != 10 && leadingByte != 13) {
        result.appendBuffer(input, tail, front - tail);
        addFix();
      } else if (leadingByte == '&' && handleEntity(input)) {
        return;
      }
    }
    result.appendBuffer(input, tail, front - tail);
    pending = null;
  }

  private void addFix() {
    tail = front + 1;
    result.appendString(REPLACEMENT_CHAR);
    numberOfFixes++;
  }

  private boolean handleEntity(Buffer input) {
    if (front == input.length() - 1) {
      incomplete(input, front + 1);
      return true;
    }
    if (input.getByte(front + 1) != '#') {
      return false;
    }
    int j;
    for (j = front + 2; j < input.length(); j++) {
      if (input.getByte(j) == ';') {
        break;
      }
    }
    if (j == input.length()) {
      incomplete(input, j);
      return true;
    }
    try {
      int v;
      if (input.getByte(front + 2) == 'x') {
        v = Integer.parseInt(input.getString(front + 3, j), 16);
      } else {
        v = Integer.parseInt(input.getString(front + 2, j));
      }
      if (v < 32 && v != 9 && v != 10 && v != 13) {
        result.appendBuffer(input, tail, front - tail);
        front = j;
        addFix();
      }
    } catch (NumberFormatException e) {
      // ignored; Data will be passed as is
    }
    return false;
  }

  private void incomplete(Buffer input, int pos) {
    if (ended) {
      result.appendBuffer(input, tail, pos - tail);
      pending = null;
    } else {
      pending = Buffer.buffer();
      pending.appendBuffer(input, tail, pos - tail);
    }
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
