package org.folio.reservoir.server;

import static org.folio.reservoir.util.EncodeXmlText.encodeXmlText;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.reservoir.module.ModuleExecutable;
import org.folio.tlib.postgres.PgCqlQuery;

public class ClusterRecordStream implements WriteStream<Row> {

  private static final Logger log = LogManager.getLogger(ClusterRecordStream.class);
  boolean ended;

  Set<Row> work = new HashSet<>();

  Storage storage;

  boolean withMetadata;

  Handler<Void> drainHandler;

  Handler<AsyncResult<Void>> endHandler;

  Handler<Throwable> exceptionHandler;

  ModuleExecutable transformer;

  WriteStream<Buffer> response;

  SqlConnection connection;

  int writeQueueMaxSize = 5;

  Vertx vertx;

  final Function<Row, Future<Buffer>> recordProcessor;

  ClusterRecordStream(
      Vertx vertx, Storage storage, SqlConnection connection,
      WriteStream<Buffer> response, ModuleExecutable transformer, boolean withMetadata,
      Function<Row, Future<Buffer>> recordProcessor) {
    this.response = response;
    this.transformer = transformer;
    this.withMetadata = withMetadata;
    this.storage = storage;
    this.connection = connection;
    this.vertx = vertx;
    this.recordProcessor = recordProcessor;
  }

  @Override
  public WriteStream<Row> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  // Future<ClusterBuilder> populateCluster(ClusterRecordItem cr) {
  //   String q = "SELECT * FROM " + storage.getGlobalRecordTable()
  //       + " LEFT JOIN " + storage.getClusterRecordTable() + " ON record_id = id "
  //       + " WHERE cluster_id = $1";
  //   return connection.preparedQuery(q)
  //       .execute(Tuple.of(cr.clusterId))
  //       .compose(rowSet -> {
  //         if (rowSet.size() == 0) {
  //           return Future.succeededFuture(null); // deleted record
  //         }
  //         ClusterBuilder cb = new ClusterBuilder(cr.clusterId).records(rowSet);
  //         if (!withMetadata) {
  //           return Future.succeededFuture(cb);
  //         }
  //         return getClusterValues(storage, connection, cr.clusterId, cb);
  //       });
  // }

  Future<Void> perform(Row row) {
    return recordProcessor.apply(row)
    .compose(buf -> response.write(buf))
    .recover(e -> {
      log.warn("Failed to produce record {} cause: {}", row.deepToString(), e.getMessage());
      log.debug(e);
      return response.write(Buffer.buffer("<!-- Failed to produce record "
          + encodeXmlText(row.deepToString()) + " cause: "
          + encodeXmlText(e.getMessage()) + " -->\n")).mapEmpty();
    });
  }

  @Override
  public Future<Void> write(Row row) {
    work.add(row);
    return perform(row).onComplete(x -> {
      work.remove(row);
      if (work.size() == writeQueueMaxSize - 1 && !ended) {
        drainHandler.handle(null);
      }
      if (work.isEmpty() && ended) {
        this.endHandler.handle(Future.succeededFuture());
      }
    });
  }

  @Override
  public void write(Row row, Handler<AsyncResult<Void>> handler) {
    write(row).onComplete(handler);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    if (ended) {
      throw new IllegalStateException("already ended");
    }
    ended = true;
    this.endHandler = handler;
    if (work.isEmpty()) {
      this.endHandler.handle(Future.succeededFuture());
    }
  }

  @Override
  public WriteStream<Row> setWriteQueueMaxSize(int i) {
    writeQueueMaxSize = i;
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return work.size() >= writeQueueMaxSize;
  }

  @Override
  public WriteStream<Row> drainHandler(@Nullable Handler<Void> handler) {
    this.drainHandler = handler;
    return this;
  }

  static Future<Void> getMarcxmlRecords(RoutingContext ctx, Storage storage, PgCqlQuery pgCqlQuery,
      int offset, int limit, Handler<String> handler) {

    return Future.failedFuture("not implemented");
    // String sqlWhere = pgCqlQuery.getWhereClause();
    // final String wClause = sqlWhere == null ? "" : " WHERE " + sqlWhere;
    // String sqlQuery = "SELECT * FROM " + storage.getClusterMetaTable() + wClause
    //     + " LIMIT " + limit + " OFFSET " + offset;
    // return getMarcxmlRecords(ctx, storage, sqlQuery, handler);
  }

  // static Future<Void> getMarcxmlRecords(RoutingContext ctx, Storage storage, String sqlQuery,
  //     Handler<String> handler) {

  //   return storage.getTransformer(ctx).compose(transformer -> {
  //     log.info("SQL Query: {}", sqlQuery);
  //     return storage.getPool()
  //         .withConnection(conn -> conn.query(sqlQuery)
  //             .execute()
  //             .compose(res -> {
  //               RowIterator<Row> iterator = res.iterator();
  //               Future<Void> future = Future.succeededFuture();
  //               while (iterator.hasNext()) {
  //                 Row row = iterator.next();
  //                 future = future.compose(x -> {
  //                   ClusterRecordItem cr = new ClusterRecordItem(row);
  //                   ClusterRecordStream clusterRecordStream = new ClusterRecordStream(
  //                       ctx.vertx(), storage, conn, null, transformer, false, null);
  //                   return clusterRecordStream.getClusterMarcXml(cr)
  //                       .map(marcxml -> {
  //                         handler.handle(marcxml);
  //                         return null;
  //                       });
  //                 });
  //               }
  //               return future;
  //             }
  //       ));
  //   });
  // }

  static Future<Integer> getTotalRecords(Storage storage, PgCqlQuery pgCqlQuery) {
    String sqlWhere = pgCqlQuery.getWhereClause();
    final String wClause = sqlWhere == null ? "" : " WHERE " + sqlWhere;
    String sqlQuery = "SELECT COUNT(*) FROM " + storage.getClusterMetaTable() + wClause;
    return storage.getPool()
        .withConnection(conn -> conn.query(sqlQuery)
            .execute()
            .map(res -> res.iterator().next().getInteger(0)));
  }

}
