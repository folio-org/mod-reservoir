package org.folio.reservoir.util.readstream;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.LinkedList;
import java.util.List;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;

public class MarcToJsonObjectMapper implements Mapper<Record, JsonObject> {

  List<JsonObject> items = new LinkedList<>();

  @Override
  public void push(Record item) {
    items.add(convert(item));
  }

  @Override
  public JsonObject poll() {
    if (items.isEmpty()) {
      return null;
    }
    return items.remove(0);
  }

  @Override
  public void end() {
    // no special end marker (whole items)
  }

  private JsonObject convert(Record in) {
    JsonObject out = new JsonObject();
    out.put("leader", in.getLeader().toString());
    JsonArray fields = new JsonArray();
    out.put("fields", fields);

    for (final ControlField cf : in.getControlFields()) {
      JsonObject field = new JsonObject();
      field.put(cf.getTag(), cf.getData());
      fields.add(field);
    }

    for (final DataField df : in.getDataFields()) {
      JsonObject field = new JsonObject();
      JsonObject tag = new JsonObject();
      field.put(df.getTag(), tag);
      JsonArray subfields = new JsonArray();
      tag.put("subfields", subfields);
      for (final Subfield sf : df.getSubfields()) {
        JsonObject subfield = new JsonObject();
        subfield.put(String.valueOf(sf.getCode()), sf.getData());
        subfields.add(subfield);
      }
      field.put("ind1", String.valueOf(df.getIndicator1()));
      field.put("ind2", String.valueOf(df.getIndicator2()));
    }
    return out;
  }
}
