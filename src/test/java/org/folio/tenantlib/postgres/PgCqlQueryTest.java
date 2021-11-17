package org.folio.tenantlib.postgres;

import org.junit.Assert;
import org.junit.Test;

public class PgCqlQueryTest {

  @Test
  public void testSimple() {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    Assert.assertNull(pgCqlQuery.getWhereClause());
    pgCqlQuery.parse(null);
    Assert.assertNull(pgCqlQuery.getWhereClause());

    pgCqlQuery.addField(new PgCqlField("title", "dc.title", PgCqlField.Type.TEXT));

    pgCqlQuery.parse("dc.Title==value");
    Assert.assertEquals("title = 'value'", pgCqlQuery.getWhereClause());

    pgCqlQuery.parse(null, "dc.Title==value2 OR dc.title==value3");
    Assert.assertEquals("(title = 'value2' OR title = 'value3')",
        pgCqlQuery.getWhereClause());

    pgCqlQuery.parse("dc.Title==value1", "dc.Title==value2 OR dc.title==value3");
    Assert.assertEquals("(title = 'value1' AND (title = 'value2' OR title = 'value3'))",
        pgCqlQuery.getWhereClause());

    pgCqlQuery.parse("dc.Title==value1 sortby title", "dc.Title==value2 OR dc.title==value3");
    Assert.assertEquals("(title = 'value1' AND (title = 'value2' OR title = 'value3'))",
        pgCqlQuery.getWhereClause());

    pgCqlQuery.addField(new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.parse("cql.allRecords = 1", "dc.title==value1");
    Assert.assertEquals("title = 'value1'",
        pgCqlQuery.getWhereClause());

    pgCqlQuery.parse("cql.allRecords = 1 sortby title", "dc.title==value1");
    Assert.assertEquals("title = 'value1'",
        pgCqlQuery.getWhereClause());
  }

  static String ftResponse(String column, String term) {
    return "to_tsvector('english', " + column + ") @@ plainto_tsquery('english', '" + term + "')";
  }

  @Test
  public void testQueries() {
    String[][] list = new String[][] {
        { "(", "error: expected index or term, got EOF" },
        { "foo=bar", "error: Unsupported CQL index: foo" },
        { "Title=v1", ftResponse("title", "v1") },
        { "Title all v1", ftResponse("title", "v1") },
        { "Title>v1", "error: Unsupported operator > for: Title > v1" },
        { "Title=\"men's room\"", ftResponse("title", "men''s room") },
        { "Title=men's room", ftResponse("title", "men''s room") },
        { "Title=v1*", "error: Masking op * unsupported for: Title = v1*" },
        { "Title=v1?", "error: Masking op ? unsupported for: Title = v1?" },
        { "Title=v1^", "error: Anchor op ^ unsupported for: Title = v1^" },
        { "Title=a\\*b", ftResponse("title", "a*b") },
        { "Title=a\\^b", ftResponse("title", "a^b") },
        { "Title=a\\?b", ftResponse("title", "a?b") },
        { "Title=a\\?b", ftResponse("title", "a?b") },
        { "Title=a\\n", ftResponse("title", "a\\n") },
        { "Title=\"a\\\"\"", ftResponse("title", "a\"") },
        { "Title=\"a\\\"b\"", ftResponse("title", "a\"b") },
        { "Title=a\\12", ftResponse("title", "a\\12") },
        { "Title=a\\\\", ftResponse("title", "a\\") },
        { "Title=a\\'", ftResponse("title", "a\\''") },
        { "Title=a\\'b", ftResponse("title", "a\\''b") },
        { "Title=a\\\\\\n", ftResponse("title", "a\\\\n") },
        { "Title=a\\\\", ftResponse("title", "a\\") },
        { "Title=aa\\\\1", ftResponse("title", "aa\\1") },
        { "Title=ab\\\\\\?", ftResponse("title", "ab\\?") },
        { "Title=\"b\\\\\"", ftResponse("title", "b\\") },
        { "Title=\"c\\\\'\"", ftResponse("title", "c\\''") },
        { "Title=\"c\\\\d\"", ftResponse("title", "c\\d") },
        { "Title=\"d\\\\\\\\\"", ftResponse("title", "d\\\\") },
        { "Title=\"x\\\\\\\"\\\\\"", ftResponse("title", "x\\\"\\") },
        { "Title=\"\"", "title IS NOT NULL" },
        { "Title<>\"\"", "title IS NULL" },
        { "Title==\"\"", "title = ''" },
        { "Title==\"*?^\"", "title = '*?^'" },
        { "Title==\"\\*\\?\\^\"", "title = '\\*\\?\\^'" },
        { "Title==\"b\\\\\"", "title = 'b\\'" },
        { "Title==\"c\\\\'\"", "title = 'c\\'''" },
        { "Title==\"d\\\\\\\\\"", "title = 'd\\\\'" },
        { "Title==\"e\\\\\\\"\\\\\"", "title = 'e\\\"\\'" },
        { "Title>\"\"", "error: Unsupported operator > for: Title > \"\"" },
        { "Title==v1 or title==v2",  "(title = 'v1' OR title = 'v2')"},
        { "isbn=978-3-16-148410-0", "isbn = '978-3-16-148410-0'" },
        { "isbn=978-3-16-148410-*", "isbn = '978-3-16-148410-*'" },
        { "cql.allRecords=1 or title==v1", null },
        { "title==v1 or cql.allRecords=1", null },
        { "Title==v1 and title==v2", "(title = 'v1' AND title = 'v2')" },
        { "Title==v1 and cql.allRecords=1", "title = 'v1'" },
        { "cql.allRecords=1 and Title==v2", "title = 'v2'" },
        { "Title==v1 not title==v2", "(title = 'v1' AND NOT title = 'v2')" },
        { "cql.allRecords=1 not title==v2", "NOT (title = 'v2')" },
        { "title==v1 not cql.allRecords=1", "FALSE" },
        { "title==v1 prox title==v2", "error: Unsupported operator PROX" },
        { "cost=1 or cost=2 and cost=3", "((cost=1 OR cost=2) AND cost=3)" }, // boolean are left-assoc and same precedence in CQL
        { "cost=1 or (cost=2 and cost=3)", "(cost=1 OR (cost=2 AND cost=3))" },
        { "cost=\"\" or cost<>\"\" not cost<>\"\"", "((cost IS NOT NULL OR cost IS NULL) AND NOT cost IS NULL)" },
        { "cost=1", "cost=1" },
        { "cost=+1.9", "cost=+1.9" },
        { "cost=e", "cost=e" },
        { "cost=1.5e3", "cost=1.5e3" },
        { "cost=-1,90", "error: Bad numeric for: cost = -1,90" },
        { "cost=0x100", "error: Bad numeric for: cost = 0x100" },
        { "cost==\"\"", "error: Bad numeric for: cost == \"\"" },
        { "cost>1", "cost>1" },
        { "cost>=2", "cost>=2" },
        { "cost==3", "cost=3" },
        { "cost<>4", "cost<>4" },
        { "cost<5", "cost<5" },
        { "cost<=6", "cost<=6" },
        { "cost adj 7", "error: Unsupported operator adj for: cost adj 7" },
        { "cost=\"\"", "cost IS NOT NULL" },
        { "paid=true", "paid=TRUE" },
        { "paid=False", "paid=FALSE" },
        { "paid=fals", "error: Bad boolean for: paid = fals" },
        { "paid=\"\"", "paid IS NOT NULL" },
        { "paid==\"\"", "error: Bad boolean for: paid == \"\"" },
        { "id=null", "error: Invalid UUID in id = null" },
        { "id==\"\"", "error: Invalid UUID in id == \"\"" },
        { "id=\"\"", "id IS NOT NULL" },
        { "id=6736bd11-5073-4026-81b5-b70b24179e02", "id='6736bd11-5073-4026-81b5-b70b24179e02'" },
        { "id=6736BD11-5073-4026-81B5-B70B24179E02", "id='6736bd11-5073-4026-81b5-b70b24179e02'" },
        { "id<>6736bd11-5073-4026-81b5-b70b24179e02", "id<>'6736bd11-5073-4026-81b5-b70b24179e02'" },
        { "title==v1 sortby cost", "title = 'v1'"},
        { ">x = \"http://foo.org/p\" title==v1", "title = 'v1'"},
    };
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(new PgCqlField("title", PgCqlField.Type.FULLTEXT));
    pgCqlQuery.addField(new PgCqlField("isbn", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(new PgCqlField("cost", PgCqlField.Type.NUMBER));
    pgCqlQuery.addField(new PgCqlField("paid", PgCqlField.Type.BOOLEAN));
    pgCqlQuery.addField(new PgCqlField("id", PgCqlField.Type.UUID));
    for (String [] entry : list) {
      String query = entry[0];
      String expect = entry[1];
      try {
        pgCqlQuery.parse(query);
        Assert.assertEquals("CQL: " + query, expect, pgCqlQuery.getWhereClause());
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(expect, "error: " + e.getMessage());
      }
    }
  }

  @Test
  public void testSort() {
    String[][] list = new String[][]{
        {"isbn=1234 sortby foo", "error: Unsupported CQL index: foo", null},
        {"paid=1234", null, null},
        {"paid=1234 sortby isbn/xx", "error: Unsupported sort modifier: xx", null},
        {"paid=1234 sortby isbn", "isbn ASC", "isbn"},
        {">dc=\"http://foo.org/p\" paid=1234 sortby isbn", "isbn ASC", "isbn"},
        {"paid=1234 sortby cost/sort.descending title/sort.ascending", "cost DESC, title ASC", "cost, title"},
    };

    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(new PgCqlField("title", PgCqlField.Type.FULLTEXT));
    pgCqlQuery.addField(new PgCqlField("isbn", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(new PgCqlField("cost", PgCqlField.Type.NUMBER));
    pgCqlQuery.addField(new PgCqlField("paid", PgCqlField.Type.BOOLEAN));
    pgCqlQuery.addField(new PgCqlField("id", PgCqlField.Type.UUID));
    for (String [] entry : list) {
      String query = entry[0];
      String expect = entry[1];
      String fields = entry[2];
      try {
        pgCqlQuery.parse(query);
        Assert.assertEquals("CQL: " + query, expect, pgCqlQuery.getOrderByClause());
        Assert.assertEquals("CQL: " + query, fields, pgCqlQuery.getOrderByFields());
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(expect, "error: " + e.getMessage());
      }
    }

  }

}
