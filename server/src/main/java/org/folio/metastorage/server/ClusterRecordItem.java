package org.folio.metastorage.server;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.folio.metastorage.server.entity.ClusterBuilder;

public class ClusterRecordItem {

  UUID clusterId;
  List<String> clusterValues;
  LocalDateTime datestamp;
  String oaiSet;
  ClusterBuilder cb;
}
