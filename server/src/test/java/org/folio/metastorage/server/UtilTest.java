package org.folio.metastorage.server;

import org.folio.okapi.testing.UtilityClassTester;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

public class UtilTest {

  @Test
  public void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(Util.class);
  }

  @Test
  public void dateParseFrom() {
    Assert.assertEquals("2022-04-12T00:00", Util.parseFrom("2022-04-12").toString());
    Assert.assertEquals("2022-04-12T10:20:30", Util.parseFrom("2022-04-12T10:20:30Z").toString());
    Assert.assertEquals("badArgument", Assert.assertThrows(OaiException.class,
        () -> Util.parseFrom("2022-04x")).getErrorCode());
  }

  @Test
  public void dateParseUntil() {
    Assert.assertEquals("2022-04-13T00:00", Util.parseUntil("2022-04-12").toString());
    Assert.assertEquals("2022-04-12T10:20:31", Util.parseUntil("2022-04-12T10:20:30Z").toString());
    Assert.assertEquals("badArgument", Assert.assertThrows(OaiException.class,
        () -> Util.parseUntil("2022-04x")).getErrorCode());
  }

  @Test
  public void datestampToFrom() {
    Assert.assertEquals("2022-04-13", Util.getNextOaiDate("2022-04-12"));
    Assert.assertEquals("2022-04-12T10:20:31", Util.getNextOaiDate("2022-04-12T10:20:30Z"));
    Assert.assertEquals("badArgument", Assert.assertThrows(OaiException.class,
        () -> Util.getNextOaiDate("2022-04x")).getErrorCode());
  }

  @Test
  public void wholeminute() {
    LocalDateTime d = LocalDateTime.of(2022, 4, 20, 16, 0, 0, 900000000);
    Assert.assertEquals("2022-04-20T16:00:00Z", Util.formatOaiDateTime(d));
  }
}
