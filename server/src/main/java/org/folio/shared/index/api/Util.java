package org.folio.shared.index.api;

import io.vertx.ext.web.validation.RequestParameter;
import io.vertx.ext.web.validation.RequestParameters;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

public final class Util {

  private Util() { }

  static String getParameterString(RequestParameter parameter) {
    return parameter == null ? null : parameter.getString();
  }

  static String getQueryParameter(RequestParameters params) {
    return Util.getParameterString(params.queryParameter("query"));
  }

  /**
   * Parse ISO time.
   * @param s time string
   * @return local date time
   */
  public static LocalDateTime parseIso(String s) {
    return LocalDateTime.parse(s, DateTimeFormatter.ISO_DATE_TIME);
  }

  /**
   * Parse OAI "from" parameter and return LocalDateTime.
   * @param from from-string
   * @return LocalDateTime representation
   * @throws OaiException if time could not be parsed
   */
  public static LocalDateTime parseFrom(String from) {
    try {
      if (from.length() == 10) {
        return parseIso(from + "T00:00:00Z");
      }
      return parseIso(from);
    } catch (DateTimeParseException e) {
      throw OaiException.badArgument("bad from " + from + ":" + e.getMessage());
    }
  }

  /**
   * Parse OAI "until" parameter and return LocalDateTime.
   * @param until until-string
   * @return parsed time with one unit above (1 second or 1 day)
   * @throws OaiException if time could not be parsed
   */
  public static LocalDateTime parseUntil(String until) {
    try {
      if (until.length() == 10) {
        return parseIso(until + "T00:00:00Z").plusDays(1L);
      }
      return parseIso(until).plusSeconds(1L);
    } catch (DateTimeParseException e) {
      throw OaiException.badArgument("bad until " + until + ":" + e.getMessage());
    }
  }

  public static String formatOaiDateTime(LocalDateTime d) {
    return d.atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS)
        .format(DateTimeFormatter.ISO_DATE_TIME);
  }
}
