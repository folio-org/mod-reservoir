package org.folio.reservoir.util.readstream;

import io.vertx.core.json.JsonObject;
import java.io.ByteArrayOutputStream;
import java.util.LinkedList;
import java.util.List;
import org.marc4j.MarcJsonWriter;
import org.marc4j.marc.Record;

public class MarcToJsonMapper implements Mapper<Record, JsonObject> {
  
  List<JsonObject> items = new LinkedList<>();
  ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  MarcJsonWriter writer = new MarcJsonWriter(buffer);

  @Override
  public void push(Record item) {
    buffer.reset();
    writer.write(item);
    items.add(new JsonObject(buffer.toString()));
    writer.close(); //does nothing for BAOS
  }

  @Override
  public JsonObject poll(boolean ended) {
    if (items.isEmpty()) {
      return null;
    }
    return items.remove(0);
  }
  
}
