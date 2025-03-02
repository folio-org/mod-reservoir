package org.folio.reservoir.server;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.postgres.PgCqlDefinition;
import org.folio.tlib.postgres.PgCqlQuery;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldAlwaysMatches;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldUuid;


public class SruService {

  private static final Logger log = LogManager.getLogger(SruService.class);

  private SruService() { }

  static Future<Void> getResponse(RoutingContext ctx, String query) {
    HttpServerResponse response = ctx.response();
    // should use createDefinitionBase
    PgCqlDefinition definition = PgCqlDefinition.create();
    definition.addField(CqlFields.CQL_ALL_RECORDS.getCqlName(), new PgCqlFieldAlwaysMatches());
    // id instead of clusterId in CqlFields.CLUSTER_ID
    definition.addField("id", new PgCqlFieldUuid().withColumn(CqlFields.CLUSTER_ID.getSqllName()));
    PgCqlQuery pgCqlQuery;
    try {
      pgCqlQuery = definition.parse(query);
    } catch (Exception e) {
      response.write("  <diagnostics>\n");
      response.write("    <diagnostic xmlns:diag=\"http://docs.oasis-open.org/ns/search-ws/diagnostic\">\n");
      response.write("      <uri>info:srw/diagnostic/1/10</uri>\n");
      response.write("      <message>Query syntax error</message>\n");
      response.write("    </diagnostic>\n");
      response.write("  </diagnostics>\n");
      return Future.succeededFuture();
    }
    // TODO: consider startRecord, maximumRecords, recordSchema
    Storage storage = new Storage(ctx);
    String sqlWhere = pgCqlQuery.getWhereClause();

    response.write("  <records>\n");
    AtomicInteger cnt = new AtomicInteger();
    return storage.getTransformer(ctx).compose(transformer -> {
      final String wClause = sqlWhere == null ? "" : " WHERE " + sqlWhere;
      String sqlQuery = "SELECT * FROM " + storage.getClusterMetaTable() + wClause;
      return storage.getPool()
          .withConnection(conn -> conn.query(sqlQuery)
              .execute()
              .compose(res -> {
                RowIterator<Row> iterator = res.iterator();
                Future<Void> future = Future.succeededFuture();
                while (iterator.hasNext()) {
                  Row row = iterator.next();
                  future = future.compose(x -> {
                    log.info("row cnt={}", cnt.get());
                    ClusterRecordItem cr = new ClusterRecordItem(row);
                    ClusterRecordStream clusterRecordStream = new ClusterRecordStream(
                        ctx.vertx(), storage, conn, response, transformer, false);
                    return clusterRecordStream.getClusterMarcXml(cr)
                        .map(marcxml -> {
                          if (marcxml == null) {
                            return null; // deleted record
                          }
                          cnt.incrementAndGet();
                          response.write("    <record>\n");
                          response.write("      <recordData>\n");
                          response.write(marcxml);
                          response.write("      </recordData>\n");
                          response.write("    </record>\n");
                          return null;
                        });
                  });
                }
                return future.onComplete(x -> {
                  response.write("  </records>\n");
                  response.write("  <numberOfRecords>" + cnt.get() + "</numberOfRecords>\n");
                });
              }));
    });
  }

  static Future<Void> get(RoutingContext ctx) {
    HttpServerResponse response = ctx.response();
    response.setChunked(true);
    response.putHeader("Content-Type", "text/xml");
    response.setStatusCode(200);

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String query = Util.getParameterString(params.queryParameter("query"));
    if (query == null) {
      // TODO: return explain response
      response.write("Query is required");
      response.end();
      return Future.succeededFuture();
    }
    response.write("<searchRetrieveResponse xmlns=\"http://docs.oasis-open.org/ns/search-ws/sruResponse\">\n");
    return getResponse(ctx, query).onComplete(x -> {
      response.write("</searchRetrieveResponse>\n");
      response.end();
    });
  }    
}
