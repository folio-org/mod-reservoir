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

public class SourceStorage {

  private SourceStorage() {}

  /**
   * Insert source; fail if source already exists.
   * @param storage tenant storage.
   * @param source to be inserted.
   * @return async result.
   */
  public static Future<Void> insert(Storage storage, Source source) {
    return storage.pool.preparedQuery(
            "INSERT INTO " + storage.sourcesTable + " (id, version)"
                + " VALUES ($1, $2)")
        .execute(Tuple.of(source.getId(), source.getVersion()))
        .mapEmpty();
  }

  /**
   * Get sources from optional query and produce HTTP response.
   * @param storage tenant storage.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> list(Storage storage, RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = Util.createPgCqlQuery();
    pgCqlQuery.addField(
        new PgCqlField("id", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(
        new PgCqlField("version", PgCqlField.Type.NUMBER));
    return new StreamResult(storage.pool, ctx, pgCqlQuery, storage.sourcesTable)
        .withArrayProp("sources")
        .result(row -> Future.succeededFuture(
            JsonObject.mapFrom(SourceRowMapper.INSTANCE.map(row))
        ));
  }

  /**
   * Get source from identifier.
   * @param storage tenant storage.
   * @param id identifier
   * @return async result with Source if found; null if not found
   */
  public static Future<Source> get(Storage storage, String id) {
    return storage.pool.preparedQuery("SELECT * FROM " + storage.sourcesTable + " WHERE id = $1")
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
   * @param storage tenant storage.
   * @param id identifier
   * @return async result with TRUE if source was found and deleted; FALSE if not found
   */
  public static Future<Boolean> delete(Storage storage, String id) {
    return storage.pool.withConnection(connection ->
        connection.preparedQuery(
                "DELETE FROM " + storage.sourcesTable + " WHERE id = $1")
            .execute(Tuple.of(id))
            .map(res -> res.rowCount() > 0));
  }

  /**
   * Create/Update source.
   * @param storage tenant storage.
   * @param source source to be created or updated
   * @return async result
   */
  public static Future<Void> update(Storage storage, Source source) {
    return storage.pool.preparedQuery(
            "INSERT INTO " + storage.sourcesTable + " (id, version)"
                + " VALUES ($1, $2) ON CONFLICT(id) DO UPDATE SET version = $2")
        .execute(Tuple.of(source.getId(), source.getVersion()))
        .mapEmpty();
  }
}
