package org.folio.reservoir.server;

import io.vertx.core.json.JsonObject;
import java.util.concurrent.atomic.AtomicInteger;


public class IngestStats {
  private final String fileName;
  private final AtomicInteger processed = new AtomicInteger();
  private final AtomicInteger ignored = new AtomicInteger();
  private final AtomicInteger inserted = new AtomicInteger();
  private final AtomicInteger updated = new AtomicInteger();
  private final AtomicInteger deleted = new AtomicInteger();

  public String getFileName() {
    return fileName;
  }

  /**
   * Create new stats for a given ingest file.
   * @param fileName ingest file name
   */
  public IngestStats(String fileName) {
    if (fileName == null) {
      throw new IllegalArgumentException("fileName cannot be null");
    }
    this.fileName = fileName;
  }

  public int incrementProcessed() {
    return processed.incrementAndGet();
  }

  public int incrementIgnored() {
    return ignored.incrementAndGet();
  }

  public int incrementInserted() {
    return inserted.incrementAndGet();
  }

  public int incrementUpdated() {
    return updated.incrementAndGet();
  }

  public int incrementDeleted() {
    return deleted.incrementAndGet();
  }

  public int processed() {
    return processed.get();
  }

  public int ignored() {
    return ignored.get();
  }

  public int inserted() {
    return inserted.get();
  }

  public int updated() {
    return updated.get();
  }

  public int deleted() {
    return deleted.get();
  }

  /**
   * Return JSON representation.
   * @return json representation
   */
  public JsonObject toJson() {
    JsonObject stats = new JsonObject();
    stats.put("processed", processed.get());
    stats.put("ignored", ignored.get());
    stats.put("inserted", inserted.get());
    stats.put("updated", updated.get());
    stats.put("deleted", deleted.get());
    return stats;
  }

}
