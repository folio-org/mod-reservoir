package org.folio.metastorage.server.entity;

import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

public class CodeModuleEntity {
  private final String id;
  private final String url;
  private final String function;

  /**
   * Create code module entity from arguments.
   * @param id local id
   * @param url url to the module
   * @param function function exported by the module
   */
  public CodeModuleEntity(String id, String url, String function) {
    this.id = id;
    this.url = url;
    this.function = function;
  }

  /**
   * Return the id.
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * Return the url.
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  /**
   * Return the function name.
   * @return the function
   */
  public String getFunction() {
    return function;
  }

  /**
   * Encode the entity as JSON.
   * @return JSON object
   */
  public JsonObject asJson() {
    return new JsonObject()
      .put(CodeModuleBuilder.ID_FIELD, id)
      .put(CodeModuleBuilder.URL_FIELD, url)
      .put(CodeModuleBuilder.FUNCTION_FIELD, function);
  }

  /**
   * Encode the entity as Tuple.
   * @return Tuple object
   */
  public Tuple asTuple() {
    return Tuple.of(id, url, function);
  }

  
  public static class CodeModuleBuilder {
    
    public static final String ID_FIELD = "id";
    
    public static final String URL_FIELD = "url";
    
    public static final String FUNCTION_FIELD = "function";
    
    private final JsonObject json;
    
    public CodeModuleBuilder(String id) {
      json = new JsonObject();
      json.put(ID_FIELD, id);
    }
    
    public CodeModuleBuilder(JsonObject source) {
      json = asJson(source);
    }
  
    public CodeModuleBuilder(Row row) {
      json = asJson(row);
    }

    public CodeModuleBuilder url(String url) {
      json.put(URL_FIELD, url);
      return this;
    }
  
    public CodeModuleBuilder function(String function) {
      json.put(FUNCTION_FIELD, function);
      return this;
    }

    /**
     * Build the entity.
     * @return entity
     */
    public CodeModuleEntity build() {
      return new CodeModuleEntity(
        json.getString(ID_FIELD),
        json.getString(URL_FIELD),
        json.getString(FUNCTION_FIELD)
      );
    }

    /*
     * A shortcut to get JSON direclty from the builder.
     */
    public JsonObject buildJson() {
      return json;
    }
  
    /**
     * Encodes a single code module row as JSON.
     * @param row code module row
     * @return JSON object
     */
    public static JsonObject asJson(Row row) {
      return new JsonObject()
        .put(ID_FIELD, row.getString(ID_FIELD))
        .put(URL_FIELD, row.getString(URL_FIELD))
        .put(FUNCTION_FIELD, row.getString(FUNCTION_FIELD));
    }
  
    /**
     * Coopies the relevat parts of the input JSON as output.
     * @param source input json
     * @return new JSON output
     */
    public static JsonObject asJson(JsonObject source) {
      return new JsonObject()
        .put(ID_FIELD, source.getString(ID_FIELD))
        .put(URL_FIELD, source.getString(URL_FIELD))
        .put(FUNCTION_FIELD, source.getString(FUNCTION_FIELD));
    }
  
  
  }

}