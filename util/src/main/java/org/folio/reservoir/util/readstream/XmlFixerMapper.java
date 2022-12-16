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

  int sequenceLength = 0; // number bytes remaining in a UTF-8 sequence

  int moved = 0;

  static boolean isAscii(byte b) {
    return (b & 128) == 0;
  }

  static boolean isContinuation(byte b) {
    return (b & 192) == 128;
  }

  static boolean is2byteSequence(byte b) {
    return (b & 224) == 192;
  }

  static boolean is3byteSequence(byte b) {
    return (b & 240) == 224;
  }

  static boolean is4byteSequence(byte b) {
    return (b & 248) == 240;
  }

  void handleSequence(Buffer input, byte leadingByte) {
    // the order of checks doesn't matter here but the most occurring
    // check is listed first. There will always be more continuation bytes
    // than leading bytes.
    if (isContinuation(leadingByte)) {
      if (sequenceLength > 0) {
        sequenceLength--;
      } else {
        skipByte(input);
      }
    } else if (is2byteSequence(leadingByte)) {
      sequenceLength = 1;
    } else if (is3byteSequence(leadingByte)) {
      sequenceLength = 2;
    } else if (is4byteSequence(leadingByte)) {
      sequenceLength = 3;
    } else {
      sequenceLength = 0;
      skipByte(input);
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
    tail = 0;
    for (front = 0; front < input.length(); front++) {
      byte leadingByte = input.getByte(front);
      if (!isAscii(leadingByte)) {
        handleSequence(input, leadingByte);
      } else if (sequenceLength > 0) {
        // bad UTF-8 sequence ... Take care of the special case where quote + gt
        // is placed after a byte which is part of a UTF-8 sequence.
        if (leadingByte == '"' && moved == 0) {
          skipByte(input);
          moved = 1;
        } else if (leadingByte == '>' && moved == 1) {
          skipByte(input);
          moved = 2;
        }
      } else {
        if (moved == 2) {
          result.appendBuffer(input, tail, front - tail);
          tail = front;
          result.appendString("\">");
          moved = 0;
        }
        if (leadingByte < 32
            && leadingByte != '\t' && leadingByte != '\r' && leadingByte != '\n') {
          result.appendBuffer(input, tail, front - tail);
          addFix();
        } else if (leadingByte == '&' && handleEntity(input)) {
          return;
        }
      }
    }
    result.appendBuffer(input, tail, front - tail);
    pending = null;
  }

  private void skipByte(Buffer input) {
    result.appendBuffer(input, tail, front - tail);
    tail = front + 1;
    numberOfFixes++;
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
      if (v < 32 && v != '\t' && v != '\r' && v != '\n') {
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
