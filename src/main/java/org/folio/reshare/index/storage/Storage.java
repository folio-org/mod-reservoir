package org.folio.reshare.index.storage;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.folio.tenantlib.postgres.TenantPgPool;
import org.folio.tenantlib.postgres.impl.TenantPgPoolImpl;


public class Storage {

  private PreparedQuery<RowSet<Row>> stmtUpsertBibRecord;
  TenantPgPool tenantPgPool;

  /**
   * Doc.
   */
  public Storage(Vertx vertx) {
    this.tenantPgPool = TenantPgPoolImpl.tenantPgPool(vertx,"diku");
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
  public  Future<Void> upsertBibRecord(
          String localIdentifier,
          String libraryId,
          String matchKey,
          JsonObject source,
          JsonObject inventory) {

    Promise<Void> promise = Promise.promise();
    stmtUpsertBibRecord.execute(
            Tuple.of(localIdentifier, libraryId, matchKey, source, inventory))
            .onComplete(result -> {
              if (result.succeeded()) {
                promise.complete();
              } else {
                promise.fail("Failed to insert into bib_record: " + result.cause().getMessage());
              }
            });
    return promise.future();
  }

}
