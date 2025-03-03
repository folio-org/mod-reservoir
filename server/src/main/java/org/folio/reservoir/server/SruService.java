package org.folio.reservoir.server;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.postgres.PgCqlDefinition;
import org.folio.tlib.postgres.PgCqlQuery;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldAlwaysMatches;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldUuid;


public class SruService {

  private static final Logger log = LogManager.getLogger(SruService.class);

  private SruService() { }


  static Future<Integer> getTotalRecords(Storage storage, PgCqlQuery pgCqlQuery) {
    String sqlWhere = pgCqlQuery.getWhereClause();
    final String wClause = sqlWhere == null ? "" : " WHERE " + sqlWhere;
    String sqlQuery = "SELECT COUNT(*) FROM " + storage.getClusterMetaTable() + wClause;
    return storage.getPool()
        .withConnection(conn -> conn.query(sqlQuery)
            .execute()
            .map(res -> res.iterator().next().getInteger(0)));
  }

  private static Future<Void> getRecords(RoutingContext ctx, Storage storage, PgCqlQuery pgCqlQuery,
      int startRecord, int maximumRecords) {

    HttpServerResponse response = ctx.response();
    String sqlWhere = pgCqlQuery.getWhereClause();
    return storage.getTransformer(ctx).compose(transformer -> {
      final String wClause = sqlWhere == null ? "" : " WHERE " + sqlWhere;
      String sqlQuery = "SELECT * FROM " + storage.getClusterMetaTable() + wClause
          + " LIMIT " + maximumRecords + " OFFSET " + (startRecord - 1);
      log.info("SQL Query: {}", sqlQuery);
      return storage.getPool()
          .withConnection(conn -> conn.query(sqlQuery)
              .execute()
              .compose(res -> {
                RowIterator<Row> iterator = res.iterator();
                Future<Void> future = Future.succeededFuture();
                while (iterator.hasNext()) {
                  Row row = iterator.next();
                  future = future.compose(x -> {
                    ClusterRecordItem cr = new ClusterRecordItem(row);
                    ClusterRecordStream clusterRecordStream = new ClusterRecordStream(
                        ctx.vertx(), storage, conn, response, transformer, false);
                    return clusterRecordStream.getClusterMarcXml(cr)
                        .map(marcxml -> {
                          if (marcxml == null) {
                            return null; // deleted record
                          }
                          response.write("    <record>\n");
                          response.write("      <recordData>\n");
                          response.write(marcxml);
                          response.write("      </recordData>\n");
                          response.write("    </record>\n");
                          return null;
                        });
                  });
                }
                return future;
              }
        ));
    });
  }

  static void  returnDiagnostics(HttpServerResponse response, String no,
      String message, String details) {
    response.write("  <diagnostics>\n");
    response.write("    <diagnostic xmlns:diag=\"http://docs.oasis-open.org/ns/search-ws/diagnostic\">\n");
    response.write("      <uri>info:srw/diagnostic/1/" + no + "</uri>\n");
    response.write("      <message>" + message + "</message>\n");
    response.write("      <details>" + details + "</details>\n");
    response.write("    </diagnostic>\n");
    response.write("  </diagnostics>\n");
  }

  static Future<Void> getSearchRetrieveResponse(RoutingContext ctx, String query) {
    HttpServerResponse response = ctx.response();
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String sruVersion = Util.getQueryParameter(params, "version");
    if (sruVersion != null && !sruVersion.equals("2.0")) {
      returnDiagnostics(response, "5", "Unsupported version", "2.0");
      return Future.succeededFuture();
    }

    Integer v = params.queryParameter("startRecord").getInteger();
    final int startRecord = v == null ? 1 : v;
    v = params.queryParameter("maximumRecords").getInteger();
    final int maximumRecords = v == null ? 10 : v;

    // should use createDefinitionBase
    PgCqlDefinition definition = PgCqlDefinition.create();
    definition.addField(CqlFields.CQL_ALL_RECORDS.getCqlName(), new PgCqlFieldAlwaysMatches());
    // id instead of clusterId in CqlFields.CLUSTER_ID
    definition.addField("id", new PgCqlFieldUuid().withColumn(CqlFields.CLUSTER_ID.getSqllName()));
    PgCqlQuery pgCqlQuery;
    try {
      pgCqlQuery = definition.parse(query);
    } catch (Exception e) {
      returnDiagnostics(response, "10", "Query syntax error", e.getMessage());
      return Future.succeededFuture();
    }
    // TODO: consider recordSchema
    Storage storage = new Storage(ctx);

    Future<Void> future = Future.succeededFuture();
    future = future.compose(x -> getTotalRecords(storage, pgCqlQuery)
        .map(totalRecords -> {
          response.write(
              "  <numberOfRecords>" + totalRecords + "</numberOfRecords>\n");
          return null;
        }));
    future = future.onComplete(x -> response.write("  <records>\n"));
    future = future.compose(x -> getRecords(ctx, storage, pgCqlQuery, startRecord, maximumRecords));
    return future.onComplete(x -> response.write("  </records>\n"));
  }

  static Future<Void> getExplainResponse(RoutingContext ctx) {
    HttpServerResponse response = ctx.response();
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);

    response.write("<explainResponse xmlns=\"http://docs.oasis-open.org/ns/search-ws/sruResponse\">\n");
    response.write("  <version>2.0</version>\n");

    final String sruVersion = Util.getQueryParameter(params, "version");
    if (sruVersion != null && !sruVersion.equals("2.0")) {
      returnDiagnostics(response, "5", "Unsupported version", "2.0");
    }
    response.write("</explainResponse>\n");
    response.end();
    return Future.succeededFuture();
  }

  static Future<Void> get(RoutingContext ctx) {
    HttpServerResponse response = ctx.response();
    response.setChunked(true);
    response.putHeader("Content-Type", "text/xml");
    response.setStatusCode(200);

    response.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String query = Util.getQueryParameter(params, "query");
    if (query == null) {
      return getExplainResponse(ctx);
    }
    response.write("<searchRetrieveResponse xmlns=\"http://docs.oasis-open.org/ns/search-ws/sruResponse\">\n");
    response.write("  <version>2.0</version>\n");
    return getSearchRetrieveResponse(ctx, query).onComplete(x -> {
      response.write("</searchRetrieveResponse>\n");
      response.end();
    });
  }
}
