package org.folio.tenantlib.postgres.impl;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class TenantPgPoolImplTest {

  static Vertx vertx;

  @BeforeClass
  public static void beforeClass() {
    vertx = Vertx.vertx();
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Before
  public void before() {
    TenantPgPoolImpl.host = null;
    TenantPgPoolImpl.port = null;
    TenantPgPoolImpl.database = null;
    TenantPgPoolImpl.user = null;
    TenantPgPoolImpl.password = null;
    TenantPgPoolImpl.maxPoolSize = null;
    TenantPgPoolImpl.serverPem = null;
    TenantPgPoolImpl.setModule("mod-a");
  }

  @Test
  public void testDefault(TestContext context) {
    TenantPgPoolImpl.setModule(null);
    context.assertNull(TenantPgPoolImpl.module);
    TenantPgPoolImpl.setModule("a-b.c");
    context.assertEquals("a_b_c", TenantPgPoolImpl.module);
    TenantPgPoolImpl tenantPgPool = TenantPgPoolImpl.tenantPgPool(vertx, "diku");
    context.assertEquals("diku_a_b_c", tenantPgPool.getSchema());
  }

  @Test
  public void testAll(TestContext context) {
    PgConnectOptions options = new PgConnectOptions();
    TenantPgPoolImpl.setDefaultConnectOptions(options);
    TenantPgPoolImpl.setModule("mod-a");
    TenantPgPoolImpl.host = "host_val";
    TenantPgPoolImpl.port = "9765";
    TenantPgPoolImpl.database = "database_val";
    TenantPgPoolImpl.user = "user_val";
    TenantPgPoolImpl.password = "password_val";
    TenantPgPoolImpl.maxPoolSize = "5";
    TenantPgPoolImpl pool = TenantPgPoolImpl.tenantPgPool(vertx, "diku");
    context.assertEquals("diku_mod_a", pool.getSchema());
    context.assertEquals("host_val", options.getHost());
    context.assertEquals(9765, options.getPort());
    context.assertEquals("database_val", options.getDatabase());
    context.assertEquals("user_val", options.getUser());
    context.assertEquals("password_val", options.getPassword());
  }

  @Test
  public void testUserDefined(TestContext context) {
    PgConnectOptions userDefined = new PgConnectOptions();
    userDefined.setHost("localhost2");
    TenantPgPoolImpl.setDefaultConnectOptions(userDefined);
    context.assertEquals(userDefined, TenantPgPoolImpl.pgConnectOptions);
    context.assertEquals("localhost2", userDefined.getHost());
    userDefined = new PgConnectOptions();
    TenantPgPoolImpl.setDefaultConnectOptions(userDefined);
    context.assertEquals(userDefined, TenantPgPoolImpl.pgConnectOptions);
    context.assertNotEquals("localhost2", userDefined.getHost());
  }

  @Test
  public void testPoolReuse(TestContext context) {
    TenantPgPoolImpl pool1 = TenantPgPoolImpl.tenantPgPool(vertx, "diku1");
    context.assertEquals("diku1_mod_a", pool1.getSchema());
    TenantPgPoolImpl pool2 = TenantPgPoolImpl.tenantPgPool(vertx, "diku2");
    context.assertEquals("diku2_mod_a", pool2.getSchema());
    context.assertNotEquals(pool1, pool2);
    context.assertEquals(pool1.pgPool, pool2.pgPool);
  }

}
