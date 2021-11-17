package org.folio.tenantlib.util;

import io.vertx.ext.web.RoutingContext;
import java.util.regex.Pattern;

public final class TenantUtil {
  // PostgreSQL names are case insensitive and must not start with a digit.
  // The maximum length is 63 characters, schema = tenant + '_' + moduleName
  // where tenant and moduleName length have a maximum of 31 each.
  private static final String TENANT_PATTERN_STRING = "^[a-z][a-z0-9]{0,30}$";
  private static final Pattern TENANT_PATTERN = Pattern.compile(TENANT_PATTERN_STRING);

  private TenantUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  /**
   * Return X-Okapi-Tenant header.
   * @throws IllegalArgumentException if header is missing or is invalid
   */
  public static String tenant(RoutingContext ctx) {
    String tenant = ctx.request().getHeader("X-Okapi-Tenant");
    if (tenant == null) {
      throw new IllegalArgumentException("X-Okapi-Tenant header is missing");
    }
    if (! TENANT_PATTERN.matcher(tenant).find()) {
      throw new IllegalArgumentException(
          "X-Okapi-Tenant header must match " + TENANT_PATTERN_STRING);
    }
    return tenant;
  }
}
