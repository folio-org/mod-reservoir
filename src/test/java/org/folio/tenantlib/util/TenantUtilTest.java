package org.folio.tenantlib.util;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.folio.okapi.testing.UtilityClassTester;
import org.junit.Test;

public class TenantUtilTest {

  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(TenantUtil.class);
  }

  private String tenant(String tenant) {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(ctx.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    return TenantUtil.tenant(ctx);
  }

  @Test
  public void valid() {
    List.of("a", "z1234567890", "abcdefghijklmnopqrstuvwxyz78901")
    .forEach(tenant -> assertThat(tenant(tenant), is(tenant)));
  }

  @Test
  public void invalidNull() {
    Throwable t = assertThrows(IllegalArgumentException.class, () -> tenant(null));
    assertThat(t.getMessage(), is("X-Okapi-Tenant header is missing"));
  }

  @Test
  public void invalid() {
    List.of("", "1", "1abc", "abcdefghijklmnopqrstuvwxyz789012").forEach(tenant -> {
      Throwable t = assertThrows(IllegalArgumentException.class, () -> tenant(tenant));
      assertThat(t.getMessage(), containsString(" must match "));
    });
  }
}
