package org.folio.reservoir.server.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.sqlclient.templates.annotations.RowMapped;

@DataObject
@RowMapped
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Source {

  private String id;

  private Integer version;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

}
