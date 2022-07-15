package org.folio.metastorage.server.entity;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class ClusterBuilder {

  public static final String GLOBAL_ID_LABEL = "globalId";
  public static final String LOCAL_ID_LABEL = "localId";
  public static final String SOURCE_ID_LABEL = "sourceId";
  public static final String SOURCE_VERSION_LABEL = "sourceVersion";
  public static final String PAYLOAD_LABEL = "payload";
  public static final String CLUSTER_ID_LABEL = "clusterId";
  public static final String DATESTAMP_LABEL = "datestamp";
  public static final String MATCH_VALUES_LABEL = "matchValues";
  public static final String RECORDS_LABEL = "records";


  private JsonObject clusterJson = new JsonObject();

  public ClusterBuilder(UUID clusterId) {
    clusterJson.put(CLUSTER_ID_LABEL, clusterId.toString());
  }

  /**
   * Set datestamp for cluster.
   * @param datestamp date stamp in ISO date time format
   * @return this
   */
  public ClusterBuilder datestamp(LocalDateTime datestamp) {
    clusterJson.put(DATESTAMP_LABEL,
        datestamp.atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
    return this;
  }

  /**
   * Set records from a RowSet.
   * @param rows row set
   * @return this
   */
  public ClusterBuilder records(RowSet<Row> rows) {
    JsonArray records = new JsonArray();
    rows.forEach(row -> records.add(encodeRecord(row)));
    return records(records);
  }

  /**
   * Set records from JsonArray.
   * @param records these are modified (sorted)
   * @return this
   */
  public ClusterBuilder records(JsonArray records) {
    Collections.sort((List<JsonObject>) records.getList(), (a, b) -> {
      int cmp = a.getString(ClusterBuilder.SOURCE_ID_LABEL)
          .compareTo(b.getString(ClusterBuilder.SOURCE_ID_LABEL));
      if (cmp != 0) {
        return cmp;
      }
      cmp = a.getInteger(ClusterBuilder.SOURCE_VERSION_LABEL)
          - b.getInteger(ClusterBuilder.SOURCE_VERSION_LABEL);
      if (cmp != 0) {
        return cmp;
      }
      return a.getString(ClusterBuilder.LOCAL_ID_LABEL)
          .compareTo(b.getString(ClusterBuilder.LOCAL_ID_LABEL));
    });
    clusterJson.put(RECORDS_LABEL, records);
    return this;
  }

  /**
   * Add matchValues from a RowSet.
   * @param rows row set
   * @return this
   */
  public ClusterBuilder matchValues(RowSet<Row> rows) {
    JsonArray matchValues = new JsonArray();
    rows.forEach(row -> matchValues.add(row.getString("match_value")));
    clusterJson.put(MATCH_VALUES_LABEL, matchValues);
    return this;
  }

  public JsonObject build() {
    return clusterJson;
  }

  /**
   * Encodes a single global record row as JSON.
   * @param row global record row
   * @return JSON encoding
   */
  public static JsonObject encodeRecord(Row row) {
    return new JsonObject()
      .put(GLOBAL_ID_LABEL, row.getUUID("id"))
      .put(LOCAL_ID_LABEL, row.getString("local_id"))
      .put(SOURCE_ID_LABEL, row.getString("source_id"))
      .put(SOURCE_VERSION_LABEL, row.getInteger("source_version"))
      .put(PAYLOAD_LABEL, row.getJsonObject(PAYLOAD_LABEL));
  }
}
