package org.folio.reservoir.server.storage;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.Tuple;
import org.folio.reservoir.server.data.Source;
import org.folio.reservoir.server.data.SourceRowMapper;
import org.folio.reservoir.server.misc.StreamResult;
import org.folio.reservoir.server.misc.Util;
import org.folio.tlib.postgres.PgCqlField;
import org.folio.tlib.postgres.PgCqlQuery;

public class SourceStorage extends Storage {

  public SourceStorage(RoutingContext routingContext) {
    super(routingContext);
  }


  /**
   * Insert source; fail if source already exists.
   * @param source to be inserted.
   * @return async result.
   */
  public Future<Void> insert(Source source) {
    return pool.preparedQuery(
            "INSERT INTO " + sourcesTable + " (id, version)"
                + " VALUES ($1, $2)")
        .execute(Tuple.of(source.getId(), source.getVersion()))
        .mapEmpty();
  }

  /**
   * Get sources from optional query and produce HTTP response.
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> list(RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = Util.createPgCqlQuery();
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("version", PgCqlField.Type.NUMBER));
    return new StreamResult(pool, ctx, pgCqlQuery, sourcesTable)
        .withArrayProp("sources")
        .result(row -> Future.succeededFuture(
            JsonObject.mapFrom(SourceRowMapper.INSTANCE.map(row))
        ));
  }

  /**
   * Get source from identifier.
   * @param id identifier
   * @return async result with Source if found; null if not found
   */
  public Future<Source> get(String id) {
    return pool.preparedQuery("SELECT * FROM " + sourcesTable + " WHERE id = $1")
        .execute(Tuple.of(id))
        .compose(res -> {
          RowIterator<Row> iterator = res.iterator();
          if (!iterator.hasNext()) {
            return Future.succeededFuture();
          }
          return Future.succeededFuture(SourceRowMapper.INSTANCE.map(iterator.next()));
        });
  }

  /**
   * Delete source.
   * @param id identifier
   * @return async result with TRUE if source was found and deleted; FALSE if not found
   */
  public Future<Boolean> delete(String id) {
    return pool.withConnection(connection ->
        connection.preparedQuery(
                "DELETE FROM " + sourcesTable + " WHERE id = $1")
            .execute(Tuple.of(id))
            .map(res -> res.rowCount() > 0));
  }

  /**
   * Create/Update source.
   * @param source source to be created or updated
   * @return async result
   */
  public Future<Void> update(Source source) {
    return pool.preparedQuery(
            "INSERT INTO " + sourcesTable + " (id, version)"
                + " VALUES ($1, $2) ON CONFLICT(id) DO UPDATE SET version = $2")
        .execute(Tuple.of(source.getId(), source.getVersion()))
        .mapEmpty();
  }
}
