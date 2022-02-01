package org.folio.reshare.index.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.postgres.TenantPgPool;

public class Storage {
  private static final Logger log = LogManager.getLogger(Storage.class);

  TenantPgPool pool;
  String bibRecordTable;
  String itemView;

  /**
   * Create storage service for tenant.
   * @param vertx Vert.x hande
   * @param tenant tenant
   */
  public Storage(Vertx vertx, String tenant) {
    this.pool = TenantPgPool.pool(vertx,tenant);
    this.bibRecordTable = pool.getSchema() + ".bib_record";
    this.itemView = pool.getSchema() + ".item_view";
  }

  /**
   * Prepares storage with tables, etc.
   * @return async result.
   */
  public Future<Void> init() {
    return pool.execute(List.of(
            "SET search_path TO " + pool.getSchema(),
            "CREATE TABLE IF NOT EXISTS " + bibRecordTable
                + "(id uuid NOT NULL PRIMARY KEY,"
                + "local_identifier VARCHAR NOT NULL,"
                + "library_id uuid NOT NULL,"
                + "title VARCHAR,"
                + "match_key VARCHAR NOT NULL,"
                + "isbn VARCHAR,"
                + "issn VARCHAR,"
                + "publisher_distributor_number VARCHAR,"
                + "source JSONB NOT NULL,"
                + "inventory JSONB"
                + ")",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_local_id ON " + bibRecordTable
                + " (local_identifier, library_id)",
            "CREATE INDEX IF NOT EXISTS idx_bib_record_match_key ON " + bibRecordTable
                + " (match_key)",
            "CREATE OR REPLACE VIEW " + itemView
                + " AS SELECT id, local_identifier, library_id, match_key,"
                + " jsonb_array_elements("
                + " (jsonb_array_elements((inventory->>'holdingsRecords')::JSONB)->>'items')::JSONB"
                + ") item FROM " + bibRecordTable
        )
    ).mapEmpty();
  }

  /**
   * Inserts or updates an entry in the shared index.
   */
  public Future<Void> upsertBibRecord(
      String localIdentifier,
      String libraryId,
      String matchKey,
      JsonObject source,
      JsonObject inventory) {

    return pool.preparedQuery(
        "INSERT INTO " + bibRecordTable
            + " (id, local_identifier, library_id, match_key, source, inventory)"
            + " VALUES ($1, $2, $3, $4, $5, $6)"
            + " ON CONFLICT (local_identifier, library_id) DO UPDATE "
            + " SET match_key = $4, "
            + "     source = $5, "
            + "     inventory = $6").execute(
        Tuple.of(UUID.randomUUID(), localIdentifier, libraryId, matchKey, source, inventory)
    ).mapEmpty();
  }

  /**
   * Get shared titles with streaming result.
   * @param ctx routing context
   * @param cqlWhere WHERE clause for SQL (null for no clause)
   * @param orderBy  ORDER BY for SQL (null for no order)
   * @return async result and result written to routing context.
   */
  public Future<Void> getTitles(RoutingContext ctx, String cqlWhere, String orderBy) {
    String from = bibRecordTable;
    if (cqlWhere != null) {
      from = from + " WHERE " + cqlWhere;
    }
    return streamResult(ctx, null, from, orderBy, "titles",
        row -> new JsonObject()
            .put("id", row.getUUID("id"))
            .put("localIdentifier", row.getString("local_identifier"))
            .put("libraryId", row.getUUID("library_id")));
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
      Function<Row, JsonObject> handler) {

    return sqlConnection.prepare(query)
        .compose(pq ->
            sqlConnection.begin().compose(tx -> {
              ctx.response().setChunked(true);
              ctx.response().putHeader("Content-Type", "application/json");
              ctx.response().write("{ \"" + property + "\" : [");
              AtomicBoolean first = new AtomicBoolean(true);
              RowStream<Row> stream = pq.createStream(50);
              stream.handler(row -> {
                if (!first.getAndSet(false)) {
                  ctx.response().write(",");
                }
                JsonObject response = handler.apply(row);
                ctx.response().write(copyWithoutNulls(response).encode());
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

  Future<Void> streamResult(
      RoutingContext ctx, String distinct,
      String from, String orderByClause, String property, Function<Row, JsonObject> handler) {

    return streamResult(ctx, distinct, distinct, List.of(from), Collections.EMPTY_LIST,
        orderByClause, property, handler);
  }

  Future<Void> streamResult(RoutingContext ctx, String distinctMain, String distinctCount,
      List<String> fromList, List<String[]> facets, String orderByClause,
      String property,
      Function<Row, JsonObject> handler) {

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
    log.info("cnt={}", countQuery.toString());
    return pool.getConnection()
        .compose(sqlConnection -> streamResult(ctx, sqlConnection, query, countQuery.toString(),
            property, facets, handler)
            .onFailure(x -> sqlConnection.close()));
  }


}
