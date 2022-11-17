package org.folio.reservoir.util.readstream;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import java.io.ByteArrayOutputStream;
import org.marc4j.MarcJsonWriter;
import org.marc4j.marc.Record;

public class MarcToJsonParser implements ReadStream<JsonObject> {

  MappingReadStream<Record, Buffer> marc4jParser;

  Handler<Throwable> exceptionHandler;

  public MarcToJsonParser(ReadStream<Buffer> stream) {
    marc4jParser = new MappingReadStream<>(stream, new Marc4jMapper());
  }

  @Override
  public ReadStream<JsonObject> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    marc4jParser.exceptionHandler(handler);
    return this;
  }

  @Override
  public ReadStream<JsonObject> handler(Handler<JsonObject> handler) {
    marc4jParser.handler(marcRecord -> {
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MarcJsonWriter writer = new MarcJsonWriter(out);
        writer.write(marcRecord);
        JsonObject marc = new JsonObject(out.toString());
        writer.close();
        handler.handle(marc);
      } catch (Exception e) {
        if (exceptionHandler != null) {
          exceptionHandler.handle(e);
        }
      }
    });
    return this;
  }

  @Override
  public ReadStream<JsonObject> pause() {
    marc4jParser.pause();
    return this;
  }

  @Override
  public ReadStream<JsonObject> resume() {
    marc4jParser.resume();
    return this;
  }

  @Override
  public ReadStream<JsonObject> fetch(long l) {
    marc4jParser.fetch(l);
    return this;
  }

  @Override
  public ReadStream<JsonObject> endHandler(Handler<Void> handler) {
    marc4jParser.endHandler(handler);
    return this;
  }
}
