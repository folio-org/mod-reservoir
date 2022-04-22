package org.folio.shared.index.api;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Base64;

public class ResumptionTokenTest {

  @Test
  public void testWithUntil() {
    LocalDateTime from = LocalDateTime.now();
    String until = "1999";
    String set = "my set";
    ResumptionToken token = new ResumptionToken(set, until);
    token.setFrom(from);
    String coded = token.encode();

    ResumptionToken token2 = new ResumptionToken(coded);
    Assert.assertEquals(from, token2.getFrom());
    Assert.assertEquals(until, token2.getUntil());
    Assert.assertEquals(set, token2.getSet());

    Assert.assertEquals("set=" + set + " from=" + from + " until=" + until, token2.toString());
  }

  @Test
  public void testNullUntil() {
    LocalDateTime from = LocalDateTime.now();
    String until = null;
    String set = "my set";
    ResumptionToken token = new ResumptionToken(set, until);
    Assert.assertEquals("set=" + set + " from=null until=" + until, token.toString());
    token.setFrom(from);

    String coded = token.encode();

    ResumptionToken token2 = new ResumptionToken(coded);
    Assert.assertEquals(from, token2.getFrom());
    Assert.assertEquals(until, token2.getUntil());
    Assert.assertEquals(set, token2.getSet());
    Assert.assertEquals("set=" + set + " from=" + from + " until=" + until, token2.toString());
  }

  @Test
  public void testFailures() {
    Assert.assertThrows(IllegalArgumentException.class, () -> new ResumptionToken("x"));

    String c1 = Base64.getEncoder().encodeToString("x".getBytes());
    Assert.assertThrows(IllegalArgumentException.class, () -> new ResumptionToken(c1));

    String c2 = Base64.getEncoder().encodeToString("x y".getBytes());
    Assert.assertThrows(IllegalArgumentException.class, () -> new ResumptionToken(c2));

    ResumptionToken token = new ResumptionToken("my set", null);
    Assert.assertThrows(IllegalStateException.class, () -> token.encode());
  }
}
