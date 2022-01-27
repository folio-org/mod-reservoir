package org.folio.reshare.index.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.folio.tlib.postgres.TenantPgPool;

public class Storage {

  private PreparedQuery<RowSet<Row>> stmtUpsertBibRecord;
  TenantPgPool tenantPgPool;

  /**
   * Doc.
   */
  public Storage(Vertx vertx, String tenant) {
    this.tenantPgPool = TenantPgPool.pool(vertx,tenant);
    this.stmtUpsertBibRecord = tenantPgPool.preparedQuery(
        "INSERT INTO {schema}.bib_record "
            + "(local_identifier, library_id, match_key, source, inventory) "
            + "VALUES ($1, $2, $3, $4, $5) "
            + "ON CONFLICT (local_identifier, library_id) DO UPDATE "
            + " SET match_key = $3, "
            + "     source = $4, "
            + "     inventory = $5");
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

    return stmtUpsertBibRecord.execute(
        Tuple.of(localIdentifier, libraryId, matchKey, source, inventory)
    ).mapEmpty();
  }

}
