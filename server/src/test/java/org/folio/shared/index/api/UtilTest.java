package org.folio.shared.index.api;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class UtilTest {

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
  public void wholeminute() {
    LocalDateTime d = LocalDateTime.of(2022, 4, 20, 16, 0, 0, 900000000);
    Assert.assertEquals("2022-04-20T16:00:00Z", Util.formatOaiDateTime(d));
  }
}
