package org.folio.reservoir.server.misc;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.postgres.PgCqlQuery;

public class StreamResult {
  private static final Logger log = LogManager.getLogger(StreamResult.class);

  private final PgPool pool;

  private final RoutingContext ctx;

  private final String from;

  private final String orderByClause;

  private String property = "items";

  private Tuple tuple = Tuple.tuple();

  private String distinctMain = null;

  private String distinctCount = null;

  private int sqlStreamFetchSize = 50;

  /**
   * Create StreamResult from CQL query.
   * @param pool PostgresQL pool
   * @param ctx Routing Context for request/response
   * @param pgCqlQuery CQL query (not parsed)
   * @param mainTable table for SQL select
   */
  public StreamResult(PgPool pool, RoutingContext ctx, PgCqlQuery pgCqlQuery, String mainTable) {
    this.pool = pool;
    this.ctx = ctx;
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    pgCqlQuery.parse(Util.getQueryParameter(params));
    String sqlWhere = pgCqlQuery.getWhereClause();
    from = sqlWhere != null ? mainTable + " WHERE " + sqlWhere : mainTable;
    orderByClause = pgCqlQuery.getOrderByClause();
  }


  /**
   * Create StreamResult from "from" part and order by clause.
   * @param pool PostgresQL pool
   * @param ctx Routing Context fore request/response
   * @param from everything after FROM
   * @param orderByClause everything after ORDER BY
   */
  public StreamResult(PgPool pool, RoutingContext ctx, String from, String orderByClause) {
    this.pool = pool;
    this.ctx = ctx;
    this.from = from;
    this.orderByClause = orderByClause;
  }

  /**
   * Set DISTINCT ON value.
   * @param distinct SQL material for DISTINCT arg
   * @return this
   */
  public StreamResult withDistinct(String distinct) {
    this.distinctMain = distinctCount = distinct;
    return this;
  }

  /**
   * Specify property for JSON array in HTTP response.
   * @param property JSON property
   * @return this
   */
  public StreamResult withArrayProp(String property) {
    this.property = property;
    return this;
  }

  /**
   * Supply tuple for FROM clause.
   * @param tuple values for prepared query
   * @return this
   */
  public StreamResult withTuple(Tuple tuple) {
    this.tuple = tuple;
    return this;
  }

  /**
   * Query and produce HTTP response.
   * @param handler Row handler producing JsonObject
   * @return async result
   */
  public Future<Void> result(Function<Row, Future<JsonObject>> handler) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    Integer offset = params.queryParameter("offset").getInteger();
    Integer limit = params.queryParameter("limit").getInteger();
    String count = params.queryParameter("count").getString();
    String query = "SELECT " + (distinctMain != null ? "DISTINCT ON (" + distinctMain + ")" : "")
        + " * FROM " + from
        + (orderByClause == null ?  "" : " ORDER BY " + orderByClause)
        + " LIMIT " + limit + " OFFSET " + offset;
    log.info("query = {}", query);
    boolean exact = "exact".equals(count);
    StringBuilder countQuery = new StringBuilder("SELECT");
    if (exact) {
      countQuery.append("(SELECT COUNT("
          + (distinctCount != null ? "DISTINCT " + distinctCount : "*")
          + ") FROM " + from + ") AS cnt0");
    }
    String countQueryString = exact ? countQuery.toString() : null;
    return pool.getConnection()
        .compose(sqlConnection -> streamResult(sqlConnection, query, countQueryString, handler)
            .onFailure(x -> sqlConnection.close()));
  }

  Future<Void> streamResult(SqlConnection sqlConnection,
      String query, String cnt, Function<Row, Future<JsonObject>> handler) {

    return sqlConnection.prepare(query)
        .compose(pq ->
            sqlConnection.begin().map(tx -> {
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
                  ctx.response().write(Util.copyWithoutNulls(response).encode());
                  stream.resume();
                });
                f.onFailure(e -> {
                  log.info("failure {}", e.getMessage(), e);
                  stream.resume();
                });
              });
              stream.endHandler(end -> {
                Future<RowSet<Row>> cntFuture = cnt != null
                    ? sqlConnection.preparedQuery(cnt).execute(tuple)
                    : Future.succeededFuture(null);
                cntFuture
                    .onSuccess(cntRes -> resultFooter(cntRes, null))
                    .onFailure(f -> {
                      log.error(f.getMessage(), f);
                      resultFooter(null, f.getMessage());
                    })
                    .eventually(x -> tx.commit().compose(y -> sqlConnection.close()));
              });
              stream.exceptionHandler(e -> {
                log.error("stream error {}", e.getMessage(), e);
                resultFooter(null, e.getMessage());
                tx.commit().compose(y -> sqlConnection.close());
              });
              return null;
            })
        );
  }

  void resultFooter(RowSet<Row> rowSet, String diagnostic) {
    JsonObject resultInfo = new JsonObject();
    if (rowSet != null) {
      int pos = 0;
      Row row = rowSet.iterator().next();
      int count = row.getInteger(pos);
      resultInfo.put("totalRecords", count);
    }
    JsonArray diagnostics = new JsonArray();
    if (diagnostic != null) {
      diagnostics.add(new JsonObject().put("message", diagnostic));
    }
    resultInfo.put("diagnostics", diagnostics);
    ctx.response().write("], \"resultInfo\": " + resultInfo.encode() + "}");
    ctx.response().end();
  }

}
