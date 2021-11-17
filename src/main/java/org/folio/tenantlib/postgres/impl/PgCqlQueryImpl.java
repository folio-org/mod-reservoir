package org.folio.tenantlib.postgres.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tenantlib.postgres.PgCqlField;
import org.folio.tenantlib.postgres.PgCqlQuery;
import org.z3950.zing.cql.CQLBooleanNode;
import org.z3950.zing.cql.CQLNode;
import org.z3950.zing.cql.CQLParseException;
import org.z3950.zing.cql.CQLParser;
import org.z3950.zing.cql.CQLPrefixNode;
import org.z3950.zing.cql.CQLSortNode;
import org.z3950.zing.cql.CQLTermNode;
import org.z3950.zing.cql.Modifier;
import org.z3950.zing.cql.ModifierSet;

public class PgCqlQueryImpl implements PgCqlQuery {
  private static final Logger log = LogManager.getLogger(PgCqlQueryImpl.class);

  final CQLParser parser = new CQLParser(CQLParser.V1POINT2);
  final Map<String, PgCqlField> fields = new HashMap<>();

  String language = "english";
  CQLNode cqlNodeRoot;

  @Override
  public void parse(String query, String q2) {
    String resultingQuery;

    try {
      if (query == null && q2 == null) {
        cqlNodeRoot = null;
        return;
      }
      if (query != null && q2 != null) {
        // get rid of sortby as it can't be combined and we don't
        // it for sorting anyway.
        CQLNode node = parser.parse(query);
        if (node instanceof CQLSortNode) {
          CQLSortNode cqlSortNode = (CQLSortNode) node;
          node = cqlSortNode.getSubtree();
        }
        resultingQuery = "(" + node.toCQL() + ") AND (" + q2 + ")";
      } else if (query != null) {
        resultingQuery = query;
      } else {
        resultingQuery = q2;
      }
      log.debug("Parsing {}", resultingQuery);
      cqlNodeRoot = parser.parse(resultingQuery);
    } catch (CQLParseException | IOException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @Override
  public void parse(String query) {
    parse(query, null);
  }

  @Override
  public String getWhereClause() {
    return handleWhere(cqlNodeRoot);
  }

  @Override
  public String getOrderByClause() {
    return handleOrderBy(cqlNodeRoot, true);
  }

  @Override
  public String getOrderByFields() {
    return handleOrderBy(cqlNodeRoot, false);
  }

  static String basicOp(CQLTermNode termNode) {
    String base = termNode.getRelation().getBase();
    switch (base) {
      case "==":
        return "=";
      case "=":
      case "<>":
        return base;
      default:
        throw new IllegalArgumentException("Unsupported operator " + base + " for: "
            + termNode.toCQL());
    }
  }

  static String numberOp(CQLTermNode termNode) {
    String base = termNode.getRelation().getBase();
    switch (base) {
      case "==":
        return "=";
      case "=":
      case "<>":
      case ">":
      case "<":
      case "<=":
      case ">=":
        return base;
      default:
        throw new IllegalArgumentException("Unsupported operator " + base + " for: "
            + termNode.toCQL());
    }
  }

  /**
   * See if this is a CQL query with a existence check (NULL or NOT NULL).
   * <p>Empty term makes "IS NULL" for CQL relation =, "IS NOT NULL" for CQL relation <>.
   * </p>
   * @param field CQL field.
   * @param termNode term.
   * @return SQL op for NULL; null if not a NULL check.
   */
  static String handleNull(PgCqlField field, CQLTermNode termNode) {
    if (!termNode.getTerm().isEmpty()) {
      return null;
    }
    String base = termNode.getRelation().getBase();
    switch (base) {
      case "=":
        return field.getColumn() + " IS NOT NULL";
      case "<>":
        return field.getColumn() + " IS NULL";
      default:
        return null;
    }
  }

  static String handleTypeUuid(PgCqlField field, CQLTermNode termNode) {
    String s = handleNull(field, termNode);
    if (s != null) {
      return s;
    }
    // convert to UUID so IllegalArgumentException is thrown if invalid
    // this also down-cases uppercase hex digits.
    try {
      UUID id = UUID.fromString(termNode.getTerm());

      String pgTerm = "'" + id + "'";
      String op = basicOp(termNode);
      return field.getColumn() + op + pgTerm;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid UUID in " + termNode.toCQL());
    }
  }

  /**
   * Convert CQL term to Postgres term - exact - without C style escapes in result.
   * <p> Double backslash is converted to backslash. Postgres quotes (') are escaped.
   * Otherwise things are passed through verbatim.
   * </p>
   * @param termNode termNode which includes term and relation.
   * @return Postgres term without C style escapes.
   */
  static String cqlTermToPgTermExact(CQLTermNode termNode) {
    String cqlTerm = termNode.getTerm();
    StringBuilder pgTerm = new StringBuilder();
    boolean backslash = false;
    for (int i = 0; i < cqlTerm.length(); i++) {
      char c = cqlTerm.charAt(i);
      if (c == '\\' && backslash) {
        backslash = false;
      } else {
        pgTerm.append(c);
        if (c == '\'') {
          pgTerm.append('\''); // important to avoid SQL injection
        }
        backslash = c == '\\';
      }
    }
    return pgTerm.toString();
  }

  /**
   * CQL full text term to Postgres term.
   * @see <a href="https://www.postgresql.org/docs/13/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS">
   *   String Constants section</a>
   *
   * <p>At this stage masking is unsupported and rejected.</p>
   * @param termNode which includes term and relation.
   * @return Postgres term.
   */
  static String cqlTermToPgTermFullText(CQLTermNode termNode) {
    String cqlTerm = termNode.getTerm();
    StringBuilder pgTerm = new StringBuilder();
    boolean backslash = false;
    for (int i = 0; i < cqlTerm.length(); i++) {
      char c = cqlTerm.charAt(i);
      // handle the CQL specials *, ?, ^, \\, rest are passed through as is
      if (c == '*') {
        if (!backslash) {
          throw new IllegalArgumentException("Masking op * unsupported for: " + termNode.toCQL());
        }
      } else if (c == '?') {
        if (!backslash) {
          throw new IllegalArgumentException("Masking op ? unsupported for: " + termNode.toCQL());
        }
      } else if (c == '^') {
        if (!backslash) {
          throw new IllegalArgumentException("Anchor op ^ unsupported for: " + termNode.toCQL());
        }
      } else if (c != '\\' && backslash) {
        pgTerm.append('\\');
      }
      if (c == '\\' && !backslash) {
        backslash = true;
      } else {
        pgTerm.append(c);
        if (c == '\'') {
          pgTerm.append(c);
        }
        backslash = false;
      }
    }
    return pgTerm.toString();
  }

  String handleTypeText(PgCqlField field, CQLTermNode termNode, boolean fullText) {
    String s = handleNull(field, termNode);
    if (s != null) {
      return s;
    }
    String base = termNode.getRelation().getBase();
    if (fullText) {
      fullText = "=".equals(base) || "all".equals(base);
    }
    if (fullText) {
      String pgTerm = cqlTermToPgTermFullText(termNode);
      return "to_tsvector('" + language + "', " + field.getColumn() + ") @@ plainto_tsquery('"
          + language + "', '" + pgTerm + "')";
    }
    return field.getColumn() + " " + basicOp(termNode)
        + " '" +  cqlTermToPgTermExact(termNode) + "'";
  }

  static String handleTypeNumber(PgCqlField field, CQLTermNode termNode) {
    String s = handleNull(field, termNode);
    if (s != null) {
      return s;
    }
    String cqlTerm = termNode.getTerm();
    if (cqlTerm.isEmpty()) {
      throw new IllegalArgumentException("Bad numeric for: " + termNode.toCQL());
    }
    for (int i = 0; i < cqlTerm.length(); i++) {
      char c = cqlTerm.charAt(i);
      switch (c) {
        case '.':
        case 'e':
        case 'E':
        case '+':
        case '-':
          break;
        default:
          if (!Character.isDigit(c)) {
            throw new IllegalArgumentException("Bad numeric for: " + termNode.toCQL());
          }
      }
    }
    return field.getColumn() + numberOp(termNode) + cqlTerm;
  }

  static String handleTypeBoolean(PgCqlField field, CQLTermNode termNode) {
    String s = handleNull(field, termNode);
    if (s != null) {
      return s;
    }
    String cqlTerm = termNode.getTerm();
    String pgTerm;
    if ("false".equalsIgnoreCase(cqlTerm)) {
      pgTerm = "FALSE";
    } else if ("true".equalsIgnoreCase(cqlTerm)) {
      pgTerm = "TRUE";
    } else {
      throw new IllegalArgumentException("Bad boolean for: " + termNode.toCQL());
    }
    return field.getColumn() + basicOp(termNode) + pgTerm;
  }

  String handleWhere(CQLNode node) {
    if (node == null) {
      return null;
    }
    if (node instanceof CQLBooleanNode) {
      CQLBooleanNode booleanNode = (CQLBooleanNode) node;
      String left = handleWhere(booleanNode.getLeftOperand());
      String right = handleWhere(booleanNode.getRightOperand());
      switch (booleanNode.getOperator()) {
        case OR:
          if (right != null && left != null) {
            return "(" + left + " OR " + right + ")";
          }
          return null;
        case AND:
          if (right != null && left != null) {
            return "(" + left + " AND " + right + ")";
          } else if (right != null) {
            return right;
          } else {
            return left;
          }
        case NOT:
          if (right != null && left != null) {
            return "(" + left + " AND NOT " + right + ")";
          } else if (right != null) {
            return "NOT (" + right + ")";
          }
          return "FALSE";
        default:
          throw new IllegalArgumentException("Unsupported operator "
              + booleanNode.getOperator().name());
      }
    } else if (node instanceof CQLTermNode) {
      CQLTermNode termNode = (CQLTermNode) node;
      PgCqlField field = fields.get(termNode.getIndex().toLowerCase());
      if (field == null) {
        throw new IllegalArgumentException("Unsupported CQL index: " + termNode.getIndex());
      }
      switch (field.getType()) {
        case ALWAYS_MATCHES:
          return null;
        case UUID:
          return handleTypeUuid(field, termNode);
        case TEXT:
          return handleTypeText(field, termNode, false);
        case FULLTEXT:
          return handleTypeText(field, termNode, true);
        case NUMBER:
          return handleTypeNumber(field, termNode);
        case BOOLEAN:
          return handleTypeBoolean(field, termNode);
        default:
          throw new IllegalArgumentException("Unsupported field type: " + field.getType().name());
      }
    } else if (node instanceof CQLSortNode) {
      CQLSortNode sortNode = (CQLSortNode) node;
      return handleWhere(sortNode.getSubtree());
    } else if (node instanceof CQLPrefixNode) {
      CQLPrefixNode prefixNode = (CQLPrefixNode) node;
      return handleWhere(prefixNode.getSubtree());
    }
    // other node types unsupported, for example proximity
    throw new IllegalArgumentException("Unsupported CQL construct: " + node.toCQL());
  }

  String handleOrderBy(CQLNode node, boolean includeOps) {
    if (node == null) {
      return null;
    }
    if (node instanceof CQLSortNode) {
      StringBuilder res = new StringBuilder();
      CQLSortNode sortNode = (CQLSortNode) node;
      for (ModifierSet modifierSet: sortNode.getSortIndexes()) {
        if (res.length() > 0) {
          res.append(", ");
        }
        PgCqlField field = fields.get(modifierSet.getBase().toLowerCase());
        if (field == null) {
          throw new IllegalArgumentException("Unsupported CQL index: " + modifierSet.getBase());
        }
        res.append(field.getColumn());
        if (includeOps) {
          res.append(" ");
          String desc = "ASC";
          for (Modifier modifier : modifierSet.getModifiers()) {
            switch (modifier.getType()) {
              case "sort.ascending":
                break;
              case "sort.descending":
                desc = "DESC";
                break;
              default:
                throw new IllegalArgumentException("Unsupported sort modifier: "
                    + modifier.getType());
            }
          }
          res.append(desc);
        }
      }
      return res.toString();
    } else if (node instanceof CQLPrefixNode) {
      CQLPrefixNode prefixNode = (CQLPrefixNode) node;
      return handleOrderBy(prefixNode.getSubtree(), includeOps);
    } else {
      log.info("node is instance of {}", node.getClass());
      return null;
    }
  }

  @Override
  public void addField(PgCqlField field) {
    fields.put(field.getName().toLowerCase(), field);
  }
}
