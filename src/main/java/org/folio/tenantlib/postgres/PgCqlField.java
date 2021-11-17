package org.folio.tenantlib.postgres;

public class PgCqlField {

  public enum Type {
    ALWAYS_MATCHES, UUID, TEXT, NUMBER, BOOLEAN, FULLTEXT
  }

  final String column;
  final String name;
  final Type type;

  /**
   * Define CQL field to Pg mapping.
   * @param name name of index in CQL  and Pg column.
   * @param type data type.
   */
  public PgCqlField(String name, Type type) {
    this(name, name, type);
  }

  /**
   * Define CQL field to Pg mapping.
   * @param column Pg column.
   * @param name name of index in CQL.
   * @param type data type.
   */
  public PgCqlField(String column, String name, Type type) {
    this.column = column;
    this.name = name;
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getColumn() {
    return column;
  }
}
