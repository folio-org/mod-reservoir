package org.folio.metastorage.server;

import io.vertx.sqlclient.Row;
import java.time.LocalDateTime;
import java.util.UUID;

public class ClusterRecordItem {

  ClusterRecordItem(Row row) {
    clusterId = row.getUUID("cluster_id");
    datestamp = row.getLocalDateTime("datestamp");
    oaiSet = row.getString("match_key_config_id");
  }

  UUID clusterId;
  LocalDateTime datestamp;
  String oaiSet;
}
