package org.folio.reshare.index.storage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import org.folio.tlib.postgres.TenantPgPool;

public class Storage {
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
            "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",
            "SET search_path TO " + pool.getSchema(),
            "CREATE TABLE IF NOT EXISTS " + bibRecordTable
                + "(id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,"
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
            "CREATE UNIQUE INDEX idx_local_id ON " + bibRecordTable
                + " (local_identifier, library_id)",
            "CREATE INDEX idx_bib_record_match_key ON " + bibRecordTable + " (match_key)",
            "CREATE VIEW " + itemView + " AS SELECT id, local_identifier, library_id, match_key,"
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
            + " (local_identifier, library_id, match_key, source, inventory)"
            + " VALUES ($1, $2, $3, $4, $5)"
            + " ON CONFLICT (local_identifier, library_id) DO UPDATE "
            + " SET match_key = $3, "
            + "     source = $4, "
            + "     inventory = $5").execute(
        Tuple.of(localIdentifier, libraryId, matchKey, source, inventory)
    ).mapEmpty();
  }

}
