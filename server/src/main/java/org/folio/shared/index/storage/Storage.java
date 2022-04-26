package org.folio.shared.index.storage;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.shared.index.matchkey.MatchKeyMethod;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.util.TenantUtil;

// Define a constant instead of duplicating this literal
@java.lang.SuppressWarnings({"squid:S1192"})
public class Storage {
  private static final Logger log = LogManager.getLogger(Storage.class);

  private static final String CREATE_IF_NO_EXISTS = "CREATE TABLE IF NOT EXISTS ";
  final TenantPgPool pool;
  final String bibRecordTable;
  final String matchKeyConfigTable;
  final String clusterRecordTable;
  final String clusterValueTable;
  final String clusterMetaTable;
  static int sqlStreamFetchSize = 50;

  /**
   * Create storage service for tenant.
   * @param vertx Vert.x hande
   * @param tenant tenant
   */
  public Storage(Vertx vertx, String tenant) {
    this.pool = TenantPgPool.pool(vertx, tenant);
    this.bibRecordTable = pool.getSchema() + ".bib_record";
    this.matchKeyConfigTable = pool.getSchema() + ".match_key_config";
    this.clusterRecordTable = pool.getSchema() + ".cluster_records";
    this.clusterValueTable = pool.getSchema() + ".cluster_values";
    this.clusterMetaTable = pool.getSchema() + ".cluster_meta";
  }

  public Storage(RoutingContext routingContext) {
    this(routingContext.vertx(), TenantUtil.tenant(routingContext));
  }

  public TenantPgPool getPool() {
    return pool;
  }

  public String getClusterMetaTable() {
    return clusterMetaTable;
  }

  public String getBibRecordTable() {
    return bibRecordTable;
  }

  public String getClusterRecordTable() {
    return clusterRecordTable;
  }

  public String getClusterValuesTable() {
    return clusterValueTable;
  }

  /**
   * Prepares storage with tables, etc.
   * @return async result.
   */
  public Future<Void> init() {
    return pool.execute(List.of(
            "SET search_path TO " + pool.getSchema(),
            CREATE_IF_NO_EXISTS + bibRecordTable
                + "(id uuid NOT NULL PRIMARY KEY,"
                + " local_id VARCHAR NOT NULL,"
                + " source_id uuid NOT NULL,"
                + " marc_payload JSONB NOT NULL,"
                + " inventory_payload JSONB"
                + ")",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_local_id ON " + bibRecordTable
                + " (local_id, source_id)",
            CREATE_IF_NO_EXISTS + matchKeyConfigTable
                + "(id VARCHAR NOT NULL PRIMARY KEY,"
                + " method VARCHAR, "
                + " update VARCHAR, "
                + " params JSONB)",
            CREATE_IF_NO_EXISTS + clusterMetaTable
                + "(cluster_id uuid NOT NULL PRIMARY KEY,"
                + " match_key_config_id VARCHAR NOT NULL,"
                + " datestamp TIMESTAMP,"
                + " FOREIGN KEY(match_key_config_id) REFERENCES " + matchKeyConfigTable
                + " ON DELETE CASCADE)",
            "CREATE INDEX IF NOT EXISTS cluster_meta_datestamp_idx ON "
                + clusterMetaTable + "(datestamp)",
            CREATE_IF_NO_EXISTS + clusterRecordTable
                + "(record_id uuid NOT NULL,"
                + " match_key_config_id VARCHAR NOT NULL,"
                + " cluster_id uuid NOT NULL,"
                + " FOREIGN KEY(match_key_config_id) REFERENCES " + matchKeyConfigTable
                + " ON DELETE CASCADE,"
                + " FOREIGN KEY(record_id) REFERENCES " + bibRecordTable + " ON DELETE CASCADE)",
            "CREATE UNIQUE INDEX IF NOT EXISTS cluster_record_record_matchkey_idx ON "
                + clusterRecordTable + "(record_id, match_key_config_id)",
            "CREATE INDEX IF NOT EXISTS cluster_record_cluster_idx ON "
                + clusterRecordTable + "(cluster_id)",
            CREATE_IF_NO_EXISTS + clusterValueTable
                + "(cluster_id uuid NOT NULL,"
                + " match_key_config_id VARCHAR NOT NULL,"
                + " match_value VARCHAR NOT NULL,"
                + " FOREIGN KEY(match_key_config_id) REFERENCES " + matchKeyConfigTable
                + " ON DELETE CASCADE)",
            "CREATE UNIQUE INDEX IF NOT EXISTS cluster_value_value_idx ON "
                + clusterValueTable + "(match_key_config_id, match_value)",
            "CREATE INDEX IF NOT EXISTS cluster_value_cluster_idx ON "
                + clusterValueTable + "(cluster_id)"
        )
    ).mapEmpty();
  }

  Future<Void> upsertBibRecord(
      SqlConnection conn,
      String localIdentifier,
      UUID sourceId,
      JsonObject marcPayload,
      JsonObject inventoryPayload,
      JsonArray matchKeyConfigs) {

    return conn.preparedQuery(
            "INSERT INTO " + bibRecordTable
                + " (id, local_id, source_id, marc_payload, inventory_payload)"
                + " VALUES ($1, $2, $3, $4, $5)"
                + " ON CONFLICT (local_id, source_id) DO UPDATE "
                + " SET marc_payload = $4, inventory_payload = $5"
                + " RETURNING id"
        )
        .execute(
            Tuple.of(UUID.randomUUID(), localIdentifier, sourceId, marcPayload, inventoryPayload)
        )
        .map(rowSet -> rowSet.iterator().next().getUUID("id"))
        .compose(id -> updateMatchKeyValues(conn, id, marcPayload, inventoryPayload,
            matchKeyConfigs))
        .mapEmpty();
  }

  Future<Void> deleteBibRecord(SqlConnection conn, String localIdentifier, UUID sourceId) {
    String q = "UPDATE " + clusterMetaTable + " AS m"
        + " SET datestamp = $3"
        + " FROM " + bibRecordTable + ", " + clusterRecordTable + " AS r"
        + " WHERE m.cluster_id = r.cluster_id AND r.record_id = id"
        + " AND local_id = $1 AND source_id = $2";
    return conn.preparedQuery(q)
        .execute(Tuple.of(localIdentifier, sourceId, LocalDateTime.now(ZoneOffset.UTC)))
        .compose(x -> conn.preparedQuery("DELETE FROM " + bibRecordTable
                + " WHERE local_id = $1 AND source_id = $2")
        .execute(Tuple.of(localIdentifier, sourceId))
        .mapEmpty());
  }

  Future<Void> upsertGlobalRecord(UUID sourceId, JsonObject globalRecord,
      JsonArray matchKeyConfigs) {

    return pool.withTransaction(conn ->
            upsertGlobalRecord(conn, sourceId, globalRecord, matchKeyConfigs))
        // addValuesToCluster may fail if for same new match key for parallel operations
        // we recover just once for that. 2nd will find the new value for the one that
        // succeeded.
        .recover(x ->
            pool.withTransaction(conn ->
                upsertGlobalRecord(conn, sourceId, globalRecord, matchKeyConfigs)));
  }

  Future<Void> upsertGlobalRecord(SqlConnection conn, UUID sourceId,
      JsonObject globalRecord, JsonArray matchKeyConfigs) {

    final String localIdentifier = globalRecord.getString("localId");
    if (Boolean.TRUE.equals(globalRecord.getBoolean("delete"))) {
      return deleteBibRecord(conn, localIdentifier, sourceId);
    }
    final JsonObject marcPayload = globalRecord.getJsonObject("marcPayload");
    final JsonObject inventoryPayload = globalRecord.getJsonObject("inventoryPayload");
    return upsertBibRecord(conn, localIdentifier, sourceId, marcPayload, inventoryPayload,
        matchKeyConfigs);
  }

  Future<Void> updateMatchKeyValues(SqlConnection conn, UUID globalId,
      JsonObject marcPayload, JsonObject inventoryPayload, JsonArray matchKeyConfigs) {
    List<Future<Void>> futures = new ArrayList<>(matchKeyConfigs.size());
    for (int i = 0; i < matchKeyConfigs.size(); i++) {
      JsonObject matchKeyConfig = matchKeyConfigs.getJsonObject(i);
      futures.add(updateMatchKeyValues(conn, globalId, marcPayload,
          inventoryPayload, matchKeyConfig));
    }
    return GenericCompositeFuture.all(futures).mapEmpty();
  }

  Future<Void> updateMatchKeyValues(SqlConnection conn, UUID globalId,
      JsonObject marcPayload, JsonObject inventoryPayload, JsonObject matchKeyConfig) {

    String update = matchKeyConfig.getString("update");
    if ("manual".equals(update)) {
      return Future.succeededFuture();
    }
    String methodName = matchKeyConfig.getString("method");
    MatchKeyMethod method = MatchKeyMethod.get(methodName);
    if (method == null) {
      return Future.failedFuture("Unknown match key method: " + methodName);
    }
    method.configure(matchKeyConfig.getJsonObject("params"));
    Set<String> keys = new HashSet<>();
    method.getKeys(marcPayload, inventoryPayload, keys);
    String matchKeyConfigId = matchKeyConfig.getString("id");
    return updateMatchKeyValues(conn, globalId, matchKeyConfigId, keys);
  }

  Future<Void> updateMatchKeyValues(SqlConnection conn, UUID globalId,
      String matchKeyConfigId, Collection<String> keys) {

    return updateClusterForRecord(conn, globalId, matchKeyConfigId, keys);
  }

  Future<Set<UUID>> updateClusterValues(SqlConnection conn, UUID newClusterId,
      String matchKeyConfigId, Collection<String> keys) {

    Set<UUID> clustersFound = new HashSet<>();
    if (keys.isEmpty()) {
      return Future.succeededFuture(clustersFound);
    }
    StringBuilder q = new StringBuilder("SELECT cluster_id, match_value FROM " + clusterValueTable
        + " WHERE match_key_config_id = $1 AND (");
    List<Object> tupleList = new ArrayList<>();
    tupleList.add(matchKeyConfigId);
    int no = 2;
    for (String key : keys) {
      if (no > 2) {
        q.append(" OR ");
      }
      q.append("match_value = $" + no++);
      tupleList.add(key);
    }
    q.append(")");
    Set<String> foundKeys = new HashSet<>();
    return conn.preparedQuery(q.toString())
        .execute(Tuple.from(tupleList))
        .map(rowSet -> {
          rowSet.forEach(row -> {
            foundKeys.add(row.getString("match_value"));
            clustersFound.add(row.getUUID("cluster_id"));
          });
          if (clustersFound.isEmpty()) {
            return newClusterId;
          } else {
            return clustersFound.iterator().next();
          }
        })
        .compose(clusterId ->
            addValuesToCluster(conn, clusterId, matchKeyConfigId, keys, foundKeys)
        )
        .map(clustersFound);
  }

  Future<Void> updateClusterForRecord(SqlConnection conn, UUID globalId,
      String matchKeyConfigId, Collection<String> keys) {

    UUID newClusterId = UUID.randomUUID();
    return updateClusterValues(conn, newClusterId, matchKeyConfigId, keys)
        .compose(clustersFound -> {
          Iterator<UUID> iterator = clustersFound.iterator();
          if (!iterator.hasNext()) {
            return createCluster(conn, newClusterId, matchKeyConfigId); // create new cluster
          }
          UUID clusterId = iterator.next();
          return updateCluster(conn, clusterId).compose(c -> {
            if (!iterator.hasNext()) {
              return Future.succeededFuture(clusterId); // exactly one already
            }
            // multiple clusters: merge remaining with this one
            return mergeClusters(conn, clusterId, iterator).map(clusterId);
          });
        })
        .compose(clusterId ->
            conn.preparedQuery("INSERT INTO " + clusterRecordTable
                    + " (record_id, match_key_config_id, cluster_id) VALUES ($1, $2, $3)"
                    + " ON CONFLICT (record_id, match_key_config_id)"
                    + " DO UPDATE SET record_id = $1, match_key_config_id = $2, cluster_id = $3")
                .execute(Tuple.of(globalId, matchKeyConfigId, clusterId))
        )
        .mapEmpty();
  }

  Future<Void> addValuesToCluster(SqlConnection conn, UUID clusterId, String matchKeyConfigId,
      Collection<String> keys, Set<String> foundKeys) {

    StringBuilder q = new StringBuilder("INSERT INTO " + clusterValueTable
        + " (cluster_id, match_key_config_id, match_value) VALUES");

    List<Object> tupleList = new ArrayList<>();
    tupleList.add(clusterId);
    tupleList.add(matchKeyConfigId);
    int no = 3;
    for (String key: keys) {
      if (!foundKeys.contains(key)) {
        if (no > 3) {
          q.append(",");
        }
        q.append(" ($1, $2, $" + no + ")");
        tupleList.add(key);
        no++;
      }
    }
    if (no == 3) {
      return Future.succeededFuture();
    }
    return conn.preparedQuery(q.toString()).execute(Tuple.from(tupleList))
        .mapEmpty();
  }

  Future<UUID> createCluster(SqlConnection conn, UUID clusterId, String matchKeyConfigId) {
    return conn.preparedQuery("INSERT INTO " + clusterMetaTable
            + " (cluster_id, datestamp, match_key_config_id) VALUES ($1, $2, $3)")
        .execute(Tuple.of(clusterId, LocalDateTime.now(ZoneOffset.UTC), matchKeyConfigId))
        .map(clusterId);
  }

  Future<Void> updateCluster(SqlConnection conn, UUID clusterId) {
    return conn.preparedQuery("UPDATE " + clusterMetaTable
            + " SET datestamp = $2 WHERE cluster_id = $1")
        .execute(Tuple.of(clusterId, LocalDateTime.now(ZoneOffset.UTC)))
        .mapEmpty();
  }

  Future<Void> mergeClusters(SqlConnection conn, UUID clusterId, Iterator<UUID> iterator) {
    StringBuilder setClause = new StringBuilder(" SET cluster_id = $1 WHERE ");
    List<UUID> tupleList = new ArrayList<>();
    tupleList.add(clusterId); // $1
    for (int no = 2; iterator.hasNext(); no++) {
      tupleList.add(iterator.next());
      if (no > 2) {
        setClause.append(" OR ");
      }
      setClause.append("cluster_id = $");
      setClause.append(no);
    }
    return conn.preparedQuery("UPDATE " + clusterValueTable + setClause)
        .execute(Tuple.from(tupleList))
        .compose(x -> conn.preparedQuery("UPDATE " + clusterRecordTable + setClause)
            .execute(Tuple.from(tupleList)))
        .mapEmpty();
    // clusterMetaTable entries not removed. These are "delete items"
  }

  /**
   * Update/insert set of global records.
   * @param request ingest record request
   * @return async result
   */
  public Future<Void> updateGlobalRecords(JsonObject request) {
    UUID sourceId = UUID.fromString(request.getString("sourceId"));
    JsonArray records = request.getJsonArray("records");

    return pool.withConnection(conn ->
        getAvailableMatchConfigs(conn).compose(matchKeyConfigs -> {
              List<Future<Void>> futures = new ArrayList<>(records.size());
              for (int i = 0; i < records.size(); i++) {
                JsonObject globalRecord = records.getJsonObject(i);
                futures.add(upsertGlobalRecord(sourceId, globalRecord, matchKeyConfigs));
              }
              return GenericCompositeFuture.all(futures).mapEmpty();
            }
        ));
  }

  Future<JsonArray> getAvailableMatchConfigs(SqlConnection conn) {
    return conn.query("SELECT * FROM " + matchKeyConfigTable)
        .execute()
        .map(res -> {
          JsonArray matchConfigs = new JsonArray();
          res.forEach(row ->
              matchConfigs.add(new JsonObject()
                  .put("id", row.getString("id"))
                  .put("method", row.getString("method"))
                  .put("params", row.getJsonObject("params"))
                  .put("update", row.getString("update"))
              ));
          return matchConfigs;
        });
  }

  static JsonObject handleRecord(Row row) {
    return new JsonObject()
        .put("globalId", row.getUUID("id"))
        .put("localId", row.getString("local_id"))
        .put("sourceId", row.getUUID("source_id"))
        .put("inventoryPayload", row.getJsonObject("inventory_payload"))
        .put("marcPayload", row.getJsonObject("marc_payload"));
  }

  /**
   * Delete global records and update timestamp.
   * @param sqlWhere SQL WHERE clause
   * @return async result
   */
  public Future<Void> deleteGlobalRecords(String sqlWhere) {
    String q = "UPDATE " + clusterMetaTable + " AS m"
        + " SET datestamp = $1"
        + " FROM " + bibRecordTable + ", " + clusterRecordTable + " AS r"
        + " WHERE m.cluster_id = r.cluster_id AND r.record_id = id";
    if (sqlWhere != null) {
      q = q + " AND " + sqlWhere;
    }
    return pool.preparedQuery(q)
        .execute(Tuple.of(LocalDateTime.now(ZoneOffset.UTC)))
        .compose(x -> {
          String from = bibRecordTable;
          if (sqlWhere != null) {
            from = from + " WHERE " + sqlWhere;
          }
          return pool.query("DELETE FROM " + from).execute();
        })
        .mapEmpty();
  }

  /**
   * Get global records.
   * @param ctx routing context
   * @param sqlWhere SQL WHERE clause
   * @param sqlOrderBy the SQL ORDER BY clause
   * @return async result
   */
  public Future<Void> getGlobalRecords(RoutingContext ctx, String sqlWhere, String sqlOrderBy) {
    String from = bibRecordTable;
    if (sqlWhere != null) {
      from = from + " WHERE " + sqlWhere;
    }
    return streamResult(ctx, null, from, sqlOrderBy, "items",
        row -> Future.succeededFuture(handleRecord(row)));
  }

  /**
   * Get cluster by cluster identifier.
   * @param clusterId cluster identifier
   * @return cluster object; null if not found
   */
  public Future<JsonObject> getClusterById(UUID clusterId) {
    return pool.withConnection(connection -> getClusterById(connection, clusterId));
  }

  Future<JsonObject> getClusterById(SqlConnection connection, UUID clusterId) {
    // get all records part of cluster and join with cluster_meta to get datestamp
    return connection.preparedQuery("SELECT * FROM " + bibRecordTable
            + " LEFT JOIN " + clusterRecordTable + " ON id = record_id"
            + " LEFT JOIN " + clusterMetaTable + " ON "
            + clusterMetaTable + ".cluster_id = " + clusterRecordTable + ".cluster_id"
            + " WHERE " + clusterRecordTable + ".cluster_id = $1")
        .execute(Tuple.of(clusterId))
        .map(rowSet -> {
          JsonArray records = new JsonArray();
          rowSet.forEach(row -> records.add(handleRecord(row)));
          JsonObject o = new JsonObject()
              .put("clusterId", clusterId.toString())
              .put("records", records);
          RowIterator<Row> iterator = rowSet.iterator();
          if (iterator.hasNext()) {
            Row row = iterator.next();
            o.put("datestamp", row.getLocalDateTime("datestamp").atZone(ZoneOffset.UTC).toString());
          }
          return o;
        })
        .compose(object -> connection.preparedQuery("SELECT * FROM " + clusterValueTable
              + " WHERE cluster_id = $1")
            .execute(Tuple.of(clusterId))
            .map(rowSet -> {
              JsonArray values = new JsonArray();
              rowSet.forEach(row -> values.add(row.getString("match_value")));
              object.put("matchValues", values);
              return object;
            }));
  }

  /**
   * return all clusters as streaming result.
   * @param ctx routing context
   * @param matchKeyId match ke config to use
   * @return async result
   */
  public Future<Void> getClusters(RoutingContext ctx, String matchKeyId,
      String sqlWhere, String sqlOrderBy) {
    String from = clusterRecordTable + " LEFT JOIN " + clusterValueTable + " ON "
        + clusterValueTable + ".cluster_id = " + clusterRecordTable + ".cluster_id"
        + " WHERE " + clusterRecordTable + ".match_key_config_id = $1";
    if (sqlWhere != null) {
      from = from + " AND (" + sqlWhere + ")";
    }
    return streamResult(ctx, clusterRecordTable + ".cluster_id", Tuple.of(matchKeyId),
        from, sqlOrderBy, "items",
        row -> getClusterById(row.getUUID("cluster_id")));
  }

  /**
   * Get global record given global identifier.
   * @param id global identifier
   * @return global record response as JSON object
   */
  public Future<JsonObject> getGlobalRecord(String id) {
    return pool.preparedQuery(
            "SELECT * FROM " + bibRecordTable + " WHERE id = $1")
        .execute(Tuple.of(id))
        .map(res -> {
          RowIterator<Row> iterator = res.iterator();
          if (!iterator.hasNext()) {
            return null;
          }
          return handleRecord(iterator.next());
        });
  }

  /**
   * Insert match key config into storage.
   * @param id match key id (user specified)
   * @param method match key method
   * @param params configuration
   * @param update strategy
   * @return async result
   */
  public Future<Void> insertMatchKeyConfig(String id, String method, JsonObject params,
      String update) {

    return pool.preparedQuery(
        "INSERT INTO " + matchKeyConfigTable + " (id, method, params, update)"
            + " VALUES ($1, $2, $3, $4)")
        .execute(Tuple.of(id, method, params, update))
        .mapEmpty();
  }

  /**
   * Update match key config into storage.
   * @param id match key id (user specified)
   * @param method match key method
   * @param params configuration
   * @param update strategy
   * @return async result with TRUE if updated; FALSE if not found
   */
  public Future<Boolean> updateMatchKeyConfig(String id, String method, JsonObject params,
      String update) {

    return pool.preparedQuery(
            "UPDATE " + matchKeyConfigTable
                + " SET method = $2, params = $3, update = $4 WHERE id = $1")
        .execute(Tuple.of(id, method, params, update))
        .map(res -> res.rowCount() > 0);
  }

  /**
   * Select match key from storage.
   * @param id match key id; null takes any first config
   * @return JSON object if found; null if not found
   */
  public Future<JsonObject> selectMatchKeyConfig(String id) {
    String q = "SELECT * FROM " + matchKeyConfigTable;
    List<String> tupleList = new ArrayList<>();
    if (id != null) {
      q = q + " WHERE id = $1";
      tupleList.add(id);
    }
    return pool.preparedQuery(q)
        .execute(Tuple.from(tupleList))
        .map(res -> {
          RowIterator<Row> iterator = res.iterator();
          if (!iterator.hasNext()) {
            return null;
          }
          Row row = iterator.next();
          return new JsonObject()
              .put("id", row.getString("id"))
              .put("method", row.getString("method"))
              .put("params", row.getJsonObject("params"))
              .put("update", row.getString("update"));
        });
  }

  /**
   * Delete match key.
   * @param id match key identifier
   * @return TRUE if deleted; FALSE if not found
   */
  public Future<Boolean> deleteMatchKeyConfig(String id) {
    return pool.withConnection(connection ->
        connection.preparedQuery(
                "DELETE FROM " + matchKeyConfigTable + " WHERE id = $1")
            .execute(Tuple.of(id))
            .map(res -> res.rowCount() > 0));
  }

  /**
   * Get match keys.
   * @param ctx routing context
   * @param sqlWhere the SQL WHERE clause
   * @param sqlOrderBy the SQL ORDER BY clause
   * @return async result
   */
  public Future<Void> getMatchKeyConfigs(RoutingContext ctx, String sqlWhere, String sqlOrderBy) {
    String from = matchKeyConfigTable;
    if (sqlWhere != null) {
      from = from + " WHERE " + sqlWhere;
    }
    return streamResult(ctx, null, from, sqlOrderBy, "matchKeys",
        row -> Future.succeededFuture(new JsonObject()
            .put("id", row.getString("id"))
            .put("method", row.getString("method"))
            .put("params", row.getJsonObject("params"))
            .put("update", row.getString("update"))
        ));
  }

  Future<JsonObject> recalculateMatchKeyValueTable(SqlConnection connection, MatchKeyMethod method,
      String matchKeyConfigId) {

    String query = "SELECT * FROM " + bibRecordTable;
    AtomicInteger count = new AtomicInteger();
    return connection.prepare(query).compose(pq ->
        connection.begin().compose(tx -> {
          RowStream<Row> stream = pq.createStream(sqlStreamFetchSize);
          Promise<JsonObject> promise = Promise.promise();
          stream.handler(row -> {
            stream.pause();
            count.incrementAndGet();

            UUID globalId = row.getUUID("id");
            Set<String> keys = new HashSet<>();
            method.getKeys(row.getJsonObject("marc_payload"),
                row.getJsonObject("inventory_payload"), keys);
            updateMatchKeyValues(connection, globalId, matchKeyConfigId, keys)
                .onFailure(e -> log.error(e.getMessage(), e))
                .onComplete(e -> stream.resume());
          });
          stream.endHandler(end -> {
            tx.commit();
            promise.complete(new JsonObject().put("count", count.get()));
          });
          stream.exceptionHandler(e -> {
            log.error(e.getMessage(), e);
            tx.commit();
            promise.fail(e);
          });
          return promise.future();
        })
    );
  }

  /**
   * Initialize match key (populate clusters).
   * @param id match key id (user specified)
   * @return statistics
   */
  public Future<JsonObject> initializeMatchKey(String id) {
    return pool.withConnection(connection ->
        connection.preparedQuery(
                "SELECT * FROM " + matchKeyConfigTable + " WHERE id = $1")
            .execute(Tuple.of(id))
            .compose(res -> {
              RowIterator<Row> iterator = res.iterator();
              if (!iterator.hasNext()) {
                return Future.succeededFuture();
              }
              Row row = iterator.next();
              String method = row.getString("method");
              JsonObject params = row.getJsonObject("params");
              MatchKeyMethod matchKeyMethod = MatchKeyMethod.get(method);
              if (matchKeyMethod == null) {
                return Future.failedFuture("Unknown match key method: " + method);
              }
              matchKeyMethod.configure(params);
              return recalculateMatchKeyValueTable(connection, matchKeyMethod, id);
            })
    );
  }

  private static JsonObject copyWithoutNulls(JsonObject obj) {
    JsonObject n = new JsonObject();
    obj.getMap().forEach((key, value) -> {
      if (value != null) {
        n.put(key, value);
      }
    });
    return n;
  }

  static void resultFooter(RoutingContext ctx, RowSet<Row> rowSet, List<String[]> facets,
      String diagnostic) {

    JsonObject resultInfo = new JsonObject();
    int count = 0;
    JsonArray facetArray = new JsonArray();
    if (rowSet != null) {
      int pos = 0;
      Row row = rowSet.iterator().next();
      count = row.getInteger(pos);
      for (String [] facetEntry : facets) {
        pos++;
        JsonObject facetObj = null;
        final String facetType = facetEntry[0];
        final String facetValue = facetEntry[1];
        for (int i = 0; i < facetArray.size(); i++) {
          facetObj = facetArray.getJsonObject(i);
          if (facetType.equals(facetObj.getString("type"))) {
            break;
          }
          facetObj = null;
        }
        if (facetObj == null) {
          facetObj = new JsonObject();
          facetObj.put("type", facetType);
          facetObj.put("facetValues", new JsonArray());
          facetArray.add(facetObj);
        }
        JsonArray facetValues = facetObj.getJsonArray("facetValues");
        facetValues.add(new JsonObject()
            .put("value", facetValue)
            .put("count", row.getInteger(pos)));
      }
    }
    resultInfo.put("totalRecords", count);
    JsonArray diagnostics = new JsonArray();
    if (diagnostic != null) {
      diagnostics.add(new JsonObject().put("message", diagnostic));
    }
    resultInfo.put("diagnostics", diagnostics);
    resultInfo.put("facets", facetArray);
    ctx.response().write("], \"resultInfo\": " + resultInfo.encode() + "}");
    ctx.response().end();
  }

  @java.lang.SuppressWarnings({"squid:S107"})  // too many arguments
  Future<Void> streamResult(RoutingContext ctx, SqlConnection sqlConnection,
      String query, String cnt, Tuple tuple, String property, List<String[]> facets,
      Function<Row, Future<JsonObject>> handler) {

    return sqlConnection.prepare(query)
        .compose(pq ->
            sqlConnection.begin().compose(tx -> {
              ctx.response().setChunked(true);
              ctx.response().putHeader("Content-Type", "application/json");
              ctx.response().write("{ \"" + property + "\" : [");
              AtomicBoolean first = new AtomicBoolean(true);
              RowStream<Row> stream = pq.createStream(sqlStreamFetchSize, tuple);
              stream.handler(row -> {
                stream.pause();
                Future<JsonObject> f = handler.apply(row);
                f.onSuccess(response -> {
                  if (!first.getAndSet(false)) {
                    ctx.response().write(",");
                  }
                  ctx.response().write(copyWithoutNulls(response).encode());
                  stream.resume();
                });
                f.onFailure(e -> {
                  log.info("failure {}", e.getMessage(), e);
                  stream.resume();
                });
              });
              stream.endHandler(end -> sqlConnection.preparedQuery(cnt).execute(tuple)
                  .onSuccess(cntRes -> resultFooter(ctx, cntRes, facets, null))
                  .onFailure(f -> {
                    log.error(f.getMessage(), f);
                    resultFooter(ctx, null, facets, f.getMessage());
                  })
                  .eventually(x -> tx.commit().compose(y -> sqlConnection.close())));
              stream.exceptionHandler(e -> {
                log.error("stream error {}", e.getMessage(), e);
                resultFooter(ctx, null, facets, e.getMessage());
                tx.commit().compose(y -> sqlConnection.close());
              });
              return Future.succeededFuture();
            })
        );
  }

  Future<Void> streamResult(RoutingContext ctx, String distinct, String from, String orderByClause,
      String property, Function<Row, Future<JsonObject>> handler) {

    return streamResult(ctx, distinct, distinct, Tuple.tuple(), List.of(from),
        Collections.emptyList(), orderByClause, property, handler);
  }

  Future<Void> streamResult(RoutingContext ctx, String distinct, Tuple tuple, String from,
      String orderByClause, String property, Function<Row, Future<JsonObject>> handler) {

    return streamResult(ctx, distinct, distinct, tuple, List.of(from), Collections.emptyList(),
        orderByClause, property, handler);
  }

  @java.lang.SuppressWarnings({"squid:S107"})  // too many arguments
  Future<Void> streamResult(RoutingContext ctx, String distinctMain, String distinctCount,
      Tuple tuple, List<String> fromList, List<String[]> facets, String orderByClause,
      String property, Function<Row, Future<JsonObject>> handler) {

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    Integer offset = params.queryParameter("offset").getInteger();
    Integer limit = params.queryParameter("limit").getInteger();
    String query = "SELECT " + (distinctMain != null ? "DISTINCT ON (" + distinctMain + ")" : "")
        + " * FROM " + fromList.get(0)
        + (orderByClause == null ?  "" : " ORDER BY " + orderByClause)
        + " LIMIT " + limit + " OFFSET " + offset;
    log.info("query={}", query);
    StringBuilder countQuery = new StringBuilder("SELECT");
    int pos = 0;
    for (String from : fromList) {
      if (pos > 0) {
        countQuery.append(",\n");
      }
      countQuery.append("(SELECT COUNT("
          + (distinctCount != null ? "DISTINCT " + distinctCount : "*")
          + ") FROM " + from + ") AS cnt" + pos);
      pos++;
    }
    log.info("cnt={}", countQuery);
    return pool.getConnection()
        .compose(sqlConnection -> streamResult(ctx, sqlConnection, query, countQuery.toString(),
            tuple, property, facets, handler)
            .onFailure(x -> sqlConnection.close()));
  }

}
