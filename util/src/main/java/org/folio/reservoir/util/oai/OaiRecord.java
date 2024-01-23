package org.folio.reservoir.util.oai;

public class OaiRecord<T> {
  String datestamp;
  String identifier;
  boolean deleted;
  T metadata;

  public String getDatestamp() {
    return datestamp;
  }

  public void setDatestamp(String datestamp) {
    this.datestamp = datestamp;
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setIsDeleted(boolean v) {
    deleted = v;
  }

  public T getMetadata() {
    return metadata;
  }

  public void setMetadata(T t) {
    metadata = t;
  }
}
