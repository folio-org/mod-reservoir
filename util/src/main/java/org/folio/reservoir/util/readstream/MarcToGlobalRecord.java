package org.folio.reservoir.util.readstream;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import java.util.LinkedList;
import java.util.List;
import org.folio.reservoir.util.MarcInJsonUtil;

public class MarcToGlobalRecord extends ReadStreamConverter<JsonObject, JsonObject> {

  List<JsonObject> marc = new LinkedList<>();

  MarcToGlobalRecord(ReadStream<JsonObject> stream) {
    super(stream);
  }

  @Override
  public void handle(JsonObject value) {
    marc.add(value);
    checkPending();
  }

  boolean isHolding(JsonObject marc) {
    return MarcInJsonUtil.lookupMarcDataField(marc, "004", null, null) != null;
  }

  /**
   * Return next global record.
   * @return null if input is incomplete; global record JSON object otherwise.
   */

  // S5413 'List.remove()' should not be used in ascending 'for' loops
  @java.lang.SuppressWarnings({"squid:S5413"})
  JsonObject getNext() {
    if (marc.isEmpty()) {
      return null;
    }
    JsonObject firstMarc = marc.get(0);
    if (isHolding(firstMarc)) {
      throw new DecodeException("Leading marc record is holding " + firstMarc.encodePrettily());
    }
    int marcSize = marc.size();
    int i = 1;
    while (i < marcSize && isHolding(marc.get(i))) {
      i++;
    }
    if (!ended && i == marcSize) {
      return null;
    }
    JsonObject payload = new JsonObject()
        .put("marc", firstMarc);
    JsonObject globalRecord = new JsonObject()
        .put("payload", payload);
    String leader = firstMarc.getString("leader");
    if (leader.length() >= 24 && leader.charAt(5) == 'd') {
      globalRecord.put("delete", true);
    }
    marc.remove(0); // remove the leader record
    if (i > 1) {
      JsonArray holdings = new JsonArray();
      for (int j = 1; j < i; j++) {
        // j < marcSize so this is safe S5413
        holdings.add(marc.remove(0)); // remove each mfhd
      }
      payload.put("marcHoldings", holdings);
    }
    return globalRecord;
  }

  @Override
  void handlePending()  {
    while (demand > 0L) {
      JsonObject next = getNext();
      if (next == null) {
        break;
      }
      if (eventHandler != null) {
        eventHandler.handle(next);
      }
      if (demand != Long.MAX_VALUE) {
        --demand;
      }
    }
  }
}
