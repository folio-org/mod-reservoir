package org.folio.metastorage.util;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.streams.ReadStream;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class XmlParserImpl implements XmlParser {

  private long demand = Long.MAX_VALUE;

  private boolean ended;

  private boolean emitting;

  private Handler<Throwable> exceptionHandler;

  private Handler<XMLStreamReader> eventHandler;

  private Handler<Void> endHandler;

  private ReadStream<Buffer> stream;

  private AsyncXMLStreamReader<AsyncByteArrayFeeder> parser;

  XmlParserImpl(ReadStream<Buffer> stream) {
    AsyncXMLInputFactory factory = new InputFactoryImpl();
    parser = factory.createAsyncForByteArray();
    this.stream = stream;
  }

  @Override
  public ReadStream<XMLStreamReader> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public ReadStream<XMLStreamReader> handler(Handler<XMLStreamReader> handler) {
    eventHandler = handler;
    if (handler != null) {
      stream.handler(this);
      stream.endHandler(v -> end());
      stream.exceptionHandler(e -> {
        if (exceptionHandler != null) {
          exceptionHandler.handle(e);
        }
      });
    } else {
      stream.handler(null);
      stream.endHandler(null);
      stream.exceptionHandler(null);
    }
    return this;
  }

  @Override
  public ReadStream<XMLStreamReader> pause() {
    demand = 0L;
    return this;
  }

  @Override
  public ReadStream<XMLStreamReader> resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public ReadStream<XMLStreamReader> fetch(long l) {
    demand += l;
    if (demand < 0L) {
      demand = Long.MAX_VALUE;
    }
    checkPending();
    return this;
  }

  @Override
  public ReadStream<XMLStreamReader> endHandler(Handler<Void> handler) {
    if (!ended) {
      endHandler = handler;
    }
    return this;
  }

  private void checkPending()  {
    if (!emitting) {
      emitting = true;
      try {
        while (demand > 0L && parser.hasNext()) {
          int event = parser.next();
          if (event == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
            break;
          }
          if (demand != Long.MAX_VALUE) {
            --demand;
          }
          if (eventHandler != null) {
            eventHandler.handle(parser);
          }
        }
        if (ended) {
          Handler<Void> handler = endHandler;
          endHandler = null;
          if (handler != null) {
            handler.handle(null);
          }
        } else {
          if (demand == 0L) {
            stream.pause();
          } else {
            stream.resume();
          }
        }
      } catch (Exception e) {
        if (exceptionHandler != null) {
          exceptionHandler.handle(e);
        } else {
          throw new DecodeException(e.getMessage(), e);
        }
      } finally {
        emitting = false;
      }
    }
  }

  @Override
  public void handle(Buffer buffer) {
    byte[] bytes = buffer.getBytes();
    try {
      parser.getInputFeeder().feedInput(bytes, 0, bytes.length);
    } catch (XMLStreamException e) {
      if (exceptionHandler != null) {
        exceptionHandler.handle(e);
      }
    }
    checkPending();
  }

  @Override
  public void end() {
    if (ended) {
      throw new IllegalStateException("Parsing already done");
    }
    ended = true;
    parser.getInputFeeder().endOfInput();
    checkPending();
  }
}
