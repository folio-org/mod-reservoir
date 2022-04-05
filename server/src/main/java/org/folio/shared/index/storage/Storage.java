package org.folio.shared.index.storage;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  TenantPgPool pool;
  String bibRecordTable;
  String matchKeyConfigTable;
  String matchKeyValueTable;
  String clusterRecordTable;
  String clusterValueTable;
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
    this.matchKeyValueTable = pool.getSchema() + ".match_key_value";
    this.clusterRecordTable = pool.getSchema() + ".cluster_records";
    this.clusterValueTable = pool.getSchema() + ".cluster_values";
  }

  public Storage(RoutingContext routingContext) {
    this(routingContext.vertx(), TenantUtil.tenant(routingContext));
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
            CREATE_IF_NO_EXISTS + matchKeyValueTable
                + "(bib_record_id uuid NOT NULL,"
                + " match_key_config_id VARCHAR NOT NULL,"
                + " match_value VARCHAR NOT NULL,"
                + " FOREIGN KEY(match_key_config_id) REFERENCES " + matchKeyConfigTable
                + " ON DELETE CASCADE,"
                + " CONSTRAINT match_key_value_fk_bib_record FOREIGN KEY "
                + "    (bib_record_id) REFERENCES " + bibRecordTable + " ON DELETE CASCADE"
                + ")",
            "CREATE UNIQUE INDEX IF NOT EXISTS match_key_value_idx ON " + matchKeyValueTable
                + " (match_key_config_id, match_value, bib_record_id)",
            "CREATE INDEX IF NOT EXISTS match_key_value_bib_id_idx ON " + matchKeyValueTable
                + " (bib_record_id)",
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
                + clusterValueTable + "(match_key_config_id, cluster_id)"
        )
    ).mapEmpty();
  }

  Future<UUID> upsertBibRecord(
      SqlConnection conn,
      String localIdentifier,
      UUID sourceId,
      JsonObject marcPayload,
      JsonObject inventoryPayload) {

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
        .map(rowSet -> rowSet.iterator().next().getUUID("id"));
  }

  Future<Void> upsertSharedRecord(SqlConnection conn, UUID sourceId,
      JsonObject sharedRecord, JsonArray matchKeyConfigs) {

    final String localIdentifier = sharedRecord.getString("localId");
    final JsonObject marcPayload = sharedRecord.getJsonObject("marcPayload");
    final JsonObject inventoryPayload = sharedRecord.getJsonObject("inventoryPayload");
    return upsertBibRecord(conn, localIdentifier, sourceId, marcPayload, inventoryPayload)
        .compose(id -> updateMatchKeyValues(conn, id,
            marcPayload, inventoryPayload, matchKeyConfigs));
  }

  Future<Void> updateMatchKeyValues(SqlConnection conn, UUID globalId,
      JsonObject marcPayload, JsonObject inventoryPayload, JsonArray matchKeyConfigs) {
    Future<Void> future = Future.succeededFuture();
    for (int i = 0; i < matchKeyConfigs.size(); i++) {
      JsonObject matchKeyConfig = matchKeyConfigs.getJsonObject(i);
      future = future.compose(x -> updateMatchKeyValues(conn, globalId,
          marcPayload, inventoryPayload, matchKeyConfig));
    }
    return Future.succeededFuture();
  }

  Future<Void> updateMatchKeyValues(SqlConnection conn, UUID globalId,
      JsonObject marcPayload, JsonObject inventoryPayload, JsonObject matchKeyConfig) {

    String update = matchKeyConfig.getString("update");
    if ("manual".equals(update)) {
      return Future.succeededFuture();
    }
    String matchKeyConfigId = matchKeyConfig.getString("id");
    String methodName = matchKeyConfig.getString("method");
    MatchKeyMethod method = MatchKeyMethod.get(methodName);
    if (method == null) {
      return Future.failedFuture("Unknown match key method: " + methodName);
    }
    method.configure(matchKeyConfig.getJsonObject("params"));
    List<String> keys = method.getKeys(marcPayload, inventoryPayload);
    return updateMatchKeyValues(conn, globalId, matchKeyConfigId, keys);
  }

  Future<Void> updateMatchKeyValues(SqlConnection conn, UUID globalId,
      String matchKeyConfigId, List<String> keys) {

    String v = System.getenv("MATCHKEY");
    Future<Void> future = Future.succeededFuture();
    if (v == null || "original".equals(v)) {
      List<Future<Void>> futures = new ArrayList<>(keys.size());
      futures.add(deleteMatchKeyValueTable(conn, matchKeyConfigId, globalId));
      for (String key : keys) {
        futures.add(upsertMatchKeyValueTable(conn, matchKeyConfigId, globalId, key));
      }
      future = GenericCompositeFuture.all(futures).mapEmpty();
    }
    if (v == null || "cluster".equals(v)) {
      future = future.compose(x -> updateClusterForRecord(conn, globalId, matchKeyConfigId, keys));
    }
    return future;
  }

  Future<Void> updateClusterForRecord(SqlConnection conn, UUID globalId,
      String matchKeyConfigId, Collection<String> keys) {

    Set<UUID> clustersFound = new HashSet<>();
    Set<String> foundKeys = new HashSet<>();
    Future<Void> future = Future.succeededFuture();
    if (!keys.isEmpty()) {
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
      future = conn.preparedQuery(q.toString())
          .execute(Tuple.from(tupleList))
          .map(rowSet -> {
            rowSet.forEach(row -> {
              foundKeys.add(row.getString("match_value"));
              clustersFound.add(row.getUUID("cluster_id"));
            });
            return null;
          });
    }
    return future
        .compose(x -> {
          Iterator<UUID> iterator = clustersFound.iterator();
          if (!iterator.hasNext()) {
            return Future.succeededFuture(UUID.randomUUID()); // create new cluster
          }
          UUID clusterId = iterator.next();
          if (!iterator.hasNext()) {
            return Future.succeededFuture(clusterId); // exactly one already
          }
          // multiple clusters: merge remaining with this one
          return mergeClusters(conn, clusterId, iterator).map(clusterId);
        })
        .compose(clusterId ->
            addValuesToCluster(conn, clusterId, matchKeyConfigId, keys, foundKeys).map(clusterId)
        )
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

    Future<Void> future = Future.succeededFuture();
    for (String key: keys) {
      if (!foundKeys.contains(key)) {
        future = future.compose(x ->
            conn.preparedQuery("INSERT INTO " + clusterValueTable
                    + " (cluster_id, match_key_config_id, match_value)"
                    + " VALUES ($1, $2, $3)")
                .execute(Tuple.of(clusterId, matchKeyConfigId, key)).mapEmpty()
        );
      }
    }
    return future;
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
            .execute(Tuple.from(tupleList))).mapEmpty();
  }

  /**
   * Upsert set of records.
   * @param request ingest record request
   * @return async result
   */
  public Future<Void> upsertSharedRecords(JsonObject request) {
    UUID sourceId = UUID.fromString(request.getString("sourceId"));
    JsonArray records = request.getJsonArray("records");

    return pool.withConnection(conn ->
        getAvailableMatchConfigs(conn).compose(matchKeyConfigs -> {
          List<Future<Void>> futures = new ArrayList<>(records.size());
          for (int i = 0; i < records.size(); i++) {
            JsonObject sharedRecord = records.getJsonObject(i);
            futures.add(upsertSharedRecord(conn, sourceId, sharedRecord, matchKeyConfigs));
          }
          return GenericCompositeFuture.all(futures).mapEmpty();
        })
    );
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

  Future<JsonObject> handleRecordWithMatchKeys(Row row) {
    JsonObject o = handleRecord(row);
    return pool.preparedQuery("SELECT * FROM " + matchKeyValueTable
            + " WHERE bib_record_id = $1")
        .execute(Tuple.of(row.getUUID("id")))
        .map(res ->  {
          JsonObject matchKeys = new JsonObject();
          res.forEach(x -> {
            String matchKeyConfig = x.getString("match_key_config_id");
            JsonArray ar = matchKeys.getJsonArray(matchKeyConfig);
            if (ar == null) {
              ar = new JsonArray();
              matchKeys.put(matchKeyConfig, ar);
            }
            ar.add(x.getString("match_value"));
          });
          o.put("matchkeys", matchKeys);
          return o;
        });
  }

  /**
   * Delete shared records and corresponding match value table entries.
   * @param sqlWhere SQL WHERE clause
   * @return async result
   */
  public Future<Void> deleteSharedRecords(String sqlWhere) {
    String from = bibRecordTable;
    if (sqlWhere != null) {
      from = from + " WHERE " + sqlWhere;
    }
    return pool.query("DELETE FROM " + from).execute().mapEmpty();
  }

  /**
   * Get shared records.
   * @param ctx routing context
   * @param sqlWhere SQL WHERE clause
   * @param sqlOrderBy the SQL ORDER BY clause
   * @return async result
   */
  public Future<Void> getSharedRecords(RoutingContext ctx, String sqlWhere, String sqlOrderBy) {
    String from = bibRecordTable;
    if (sqlWhere != null) {
      from = from + " WHERE " + sqlWhere;
    }
    return streamResult(ctx, null, from, sqlOrderBy, "items", this::handleRecordWithMatchKeys);
  }

  private Future<Map<UUID, JsonObject>> getClusterResult(SqlConnection connection,
      Map<String, Set<String>> matchKeys,  List<String> matchKeyIds,
      int maxIterations, RowSet<Row> res) {

    Map<UUID,JsonObject> records = new HashMap<>();
    if (res.rowCount() > 50) {
      return Future.failedFuture("getCluster can not start with more than 50 records");
    }
    List<Future<Void>> futures = new ArrayList<>(res.rowCount());
    res.forEach(row -> futures.add(handleRecordWithMatchKeys(row)
        .map(j -> {
          records.put(row.getUUID("id"), j);
          return null;
        })));
    return GenericCompositeFuture.all(futures).compose(res1 -> {
      AtomicBoolean added = new AtomicBoolean();
      records.forEach((k, v) -> {
        JsonObject m = v.getJsonObject("matchkeys");
        m.fieldNames().forEach(f -> {
          if (matchKeyIds == null || matchKeyIds.contains(f)) {
            matchKeys.computeIfAbsent(f, x -> new HashSet<>());
            m.getJsonArray(f).forEach(e -> {
              if (matchKeys.get(f).add((String) e)) {
                added.set(true);
              }
            });
          }
        });
      });
      if (added.get() && maxIterations > 0) {
        return getCluster2(connection, matchKeys, maxIterations - 1);
      }
      return Future.succeededFuture(records);
    });
  }

  private Future<Map<UUID, JsonObject>> getCluster2(SqlConnection connection,
      Map<String, Set<String>> matchKeys, int maxIterations) {

    StringBuilder q = new StringBuilder("SELECT * FROM " + bibRecordTable
        + " INNER JOIN " + matchKeyValueTable
        + " ON bib_record_id = " + bibRecordTable + ".id WHERE ");
    int t = 1;
    List<Object> tupleValues = new ArrayList<>();
    for (Map.Entry<String,Set<String>> entry : matchKeys.entrySet()) {
      for (String v : entry.getValue()) {
        if (t > 1) {
          q.append(" OR ");
        }
        q.append("(match_key_config_id = $" + t + " AND match_value = $" + (t + 1) + ")");
        tupleValues.add(entry.getKey());
        tupleValues.add(v);
        t += 2;
      }
    }
    return connection.preparedQuery(q.toString())
        .execute(Tuple.from(tupleValues))
        .compose(res -> getClusterResult(connection, matchKeys, null, maxIterations, res));
  }

  /**
   * Get cluster from CQL where start and with given match key config IDs in use.
   * @param sqlWhere SQL where clause for shared records
   * @param matchKeyIds match key identifications
   * @param maxIterations for finding related records
   * @return map of shared records
   */
  public Future<Map<UUID,JsonObject>> getCluster(String sqlWhere, List<String> matchKeyIds,
      int maxIterations) {

    String from = bibRecordTable;
    if (sqlWhere != null) {
      from = from + " WHERE " + sqlWhere;
    }
    String q = "SELECT * FROM " + from;
    return pool.withConnection(connection ->
        connection.query(q)
            .execute()
            .compose(res ->
                getClusterResult(connection, new HashMap<>(), matchKeyIds, maxIterations, res))
    );
  }

  Future<JsonObject> getClusterById(SqlConnection connection, UUID clusterId) {
    return connection.preparedQuery("SELECT * FROM " + bibRecordTable
            + " LEFT JOIN " + clusterRecordTable + " ON id = record_id"
            + " WHERE cluster_id = $1")
        .execute(Tuple.of(clusterId))
        .map(rowSet -> {
          JsonArray records = new JsonArray();
          rowSet.forEach(row -> records.add(handleRecord(row)));
          return new JsonObject()
              .put("clusterId", clusterId.toString())
              .put("records", records);
        });
  }

  /**
   * return all clusters as streaming result.
   * @param ctx routing context
   * @param matchKeyConfigId match ke config to use
   * @return async result
   */
  public Future<Void> getAllClusters(RoutingContext ctx, String matchKeyConfigId) {
    String q = "SELECT DISTINCT ON(cluster_id) cluster_id FROM " + clusterRecordTable
        + " WHERE match_key_config_id = '" + matchKeyConfigId + "'";

    return pool.getConnection().compose(connection ->
        connection.prepare(q)
            .onFailure(pq -> connection.close())
            .compose(pq -> connection.begin().compose(tx -> {
              HttpServerResponse response = ctx.response();
              response.setChunked(true);
              response.setStatusCode(200);
              response.putHeader("Content-Type", "application/json");
              RowStream<Row> stream = pq.createStream(sqlStreamFetchSize);
              response.write("{ \"items\" : [\n");
              AtomicInteger cnt = new AtomicInteger();
              stream.handler(row -> {
                if (cnt.incrementAndGet() > 1) {
                  response.write(",\n");
                }
                UUID clusterId = row.getUUID("cluster_id");
                stream.pause();
                getClusterById(connection, clusterId)
                    .onFailure(e -> {
                      log.error(e.getMessage(), e);
                      stream.close();
                    })
                    .onSuccess(obj -> {
                      response.write(obj.encodePrettily());
                      stream.resume();
                    });
                if ((cnt.get() % 1000) == 0) {
                  log.info("cnt = {}", cnt);
                }
              });
              stream.endHandler(end -> {
                tx.commit().compose(y -> connection.close());
                response.write("\n]\n}");
                response.end();
              });
              stream.exceptionHandler(e -> {
                log.error("stream error {}", e.getMessage(), e);
                tx.commit().compose(y -> connection.close());
              });
              return Future.succeededFuture();
            })));
  }

  /**
   * Select shared record given global identifier.
   * @param id global identifier
   * @return shared record response as JSON object
   */
  public Future<JsonObject> selectSharedRecord(String id) {
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
   * @param id match key id (user specified)
   * @return JSON object if found; null if not found
   */
  public Future<JsonObject> selectMatchKeyConfig(String id) {
    return pool.preparedQuery(
            "SELECT * FROM " + matchKeyConfigTable + " WHERE id = $1")
        .execute(Tuple.of(id))
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

  Future<Void> deleteMatchKeyValueTable(SqlConnection connection, String matchKeyMethodId,
      UUID globalId) {

    return connection.preparedQuery("DELETE FROM " + matchKeyValueTable
        + " WHERE match_key_config_id = $1 AND bib_record_id = $2").execute(
        Tuple.of(matchKeyMethodId, globalId)).mapEmpty();
  }

  Future<Void> upsertMatchKeyValueTable(SqlConnection connection, String matchKeyMethodId,
      UUID globalId, String key) {
    return connection.preparedQuery("INSERT INTO " + matchKeyValueTable
        + " (bib_record_id, match_key_config_id, match_value)"
        + " VALUES ($1, $2, $3)")
        .execute(
        Tuple.of(globalId, matchKeyMethodId, key)).mapEmpty();
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
            List<String> keys = method.getKeys(row.getJsonObject("marc_payload"),
                row.getJsonObject("inventory_payload"));
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

  Future<Void> streamResult(RoutingContext ctx, SqlConnection sqlConnection,
      String query, String cnt, String property, List<String[]> facets,
      Function<Row, Future<JsonObject>> handler) {

    return sqlConnection.prepare(query)
        .compose(pq ->
            sqlConnection.begin().compose(tx -> {
              ctx.response().setChunked(true);
              ctx.response().putHeader("Content-Type", "application/json");
              ctx.response().write("{ \"" + property + "\" : [");
              AtomicBoolean first = new AtomicBoolean(true);
              RowStream<Row> stream = pq.createStream(sqlStreamFetchSize);
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
              stream.endHandler(end -> sqlConnection.query(cnt).execute()
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

    return streamResult(ctx, distinct, distinct, List.of(from), Collections.emptyList(),
        orderByClause, property, handler);
  }

  @java.lang.SuppressWarnings({"squid:S107"})  // too many arguments
  Future<Void> streamResult(RoutingContext ctx, String distinctMain, String distinctCount,
      List<String> fromList, List<String[]> facets, String orderByClause,
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
            property, facets, handler)
            .onFailure(x -> sqlConnection.close()));
  }

}
