package org.folio.tenantlib.postgres;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tenantlib.postgres.impl.TenantPgPoolImpl;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

@RunWith(VertxUnitRunner.class)
public class TenantPgPoolTest {
  private final static Logger log = LogManager.getLogger(TenantPgPoolTest.class);

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  static final String KEY_PATH = "/var/lib/postgresql/data/server.key";
  static final String CRT_PATH = "/var/lib/postgresql/data/server.crt";
  static final String CONF_PATH = "/var/lib/postgresql/data/postgresql.conf";
  static final String CONF_BAK_PATH = "/var/lib/postgresql/data/postgresql.conf.bak";

  static Vertx vertx;

  // execute commands in container (stolen from Okapi's PostgresHandleTest
  static void exec(String... command) {
    try {
      Container.ExecResult execResult = postgresSQLContainer.execInContainer(command);
      if (execResult.getExitCode() != 0) {
        log.info("{} {}", execResult.getExitCode(), String.join(" ", command));
        log.info("stderr: {}", execResult.getStderr());
        log.info("stdout: {}", execResult.getStdout());
      }
    } catch (InterruptedException|IOException|UnsupportedOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Append each entry to postgresql.conf and reload it into postgres.
   * Appending a key=value entry has precedence over any previous entries of the same key.
   * Stolen from Okapi's PostgresHandleTest
   */
  static void configure(String... configEntries) {
    exec("cp", "-p", CONF_BAK_PATH, CONF_PATH);  // start with unaltered config
    for (String configEntry : configEntries) {
      exec("sh", "-c", "echo '" + configEntry + "' >> " + CONF_PATH);
    }
    exec("su-exec", "postgres", "pg_ctl", "reload");
  }

  @BeforeClass
  public static void beforeClass() {
    vertx = Vertx.vertx();

    MountableFile serverKeyFile = MountableFile.forClasspathResource("server.key");
    MountableFile serverCrtFile = MountableFile.forClasspathResource("server.crt");
    postgresSQLContainer.copyFileToContainer(serverKeyFile, KEY_PATH);
    postgresSQLContainer.copyFileToContainer(serverCrtFile, CRT_PATH);
    exec("chown", "postgres.postgres", KEY_PATH, CRT_PATH);
    exec("chmod", "400", KEY_PATH, CRT_PATH);
    exec("cp", "-p", CONF_PATH, CONF_BAK_PATH);
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Before
  public void before() {
    configure();  // default postgresql.conf
    TenantPgPool.setServerPem(null);
    TenantPgPool.setModule("mod-foo");
  }

  /**
   * Create a TenantPgPool, run the mapper on it, close the pool,
   * and return the result from the mapper.
   */
  private <T> Future<T> withPool(Function<TenantPgPool, Future<T>> mapper) {
    TenantPgPool pool = TenantPgPool.pool(vertx, "diku");
    Future<T> future = mapper.apply(pool);
    return future.eventually(x -> pool.close());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadModule() {
    TenantPgPoolImpl.setModule("mod'a");
  }

  @Test(expected = IllegalStateException.class)
  public void testNoSetModule() {
    TenantPgPoolImpl.setModule(null);
    TenantPgPoolImpl.tenantPgPool(vertx, "diku");
  }

  @Test
  public void queryOk(TestContext context) {
    withPool(pool -> pool
        .query("SELECT count(*) FROM pg_database")
        .execute())
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void preparedQueryOk(TestContext context) {
    withPool(pool -> pool
        .preparedQuery("SELECT * FROM pg_database WHERE datname=$1")
        .execute(Tuple.of("postgres")))
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void executeOk(TestContext context) {
    withPool(pool -> pool
        .execute("SELECT * FROM pg_database WHERE datname=$1", Tuple.of("postgres")))
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void executeAnalyze(TestContext context) {
    vertx.getOrCreateContext().config().put("explain_analyze", Boolean.TRUE);
    withPool(pool -> pool
        .execute("SELECT * FROM pg_database WHERE datname=$1", Tuple.of("postgres")))
        .onComplete(x -> {
          vertx.getOrCreateContext().config().remove("explain_analyze");
          context.assertTrue(x.succeeded());
        });
  }

  @Test
  public void applicationName(TestContext context) {
    withPool(pool -> pool
        .query("SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()")
        .execute())
    .onComplete(context.asyncAssertSuccess(rowSet -> {
      assertThat(rowSet.iterator().next().getString(0), is("mod_foo"));
    }));
  }

  @Test
  public void getConnection1(TestContext context) {
    withPool(pool -> pool.withConnection(con ->
        con.query("SELECT count(*) FROM pg_database").execute()))
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void getConnection2(TestContext context) {
    withPool(pool ->
        Future.<SqlConnection>future(promise -> pool.getConnection(promise))
        .compose(con -> con.query("SELECT count(*) FROM pg_database").execute()))
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void execute1(TestContext context) {
    List<String> list = new LinkedList<>();
    list.add("CREATE TABLE a (year int)");
    list.add("SELECT * FROM a");
    list.add("DROP TABLE a");
    withPool(pool -> pool.execute(list))
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void execute2(TestContext context) {
    // execute not using a transaction as this test shows.
    List<String> list = new LinkedList<>();
    list.add("CREATE TABLE a (year int)");
    list.add("ALTER TABLE a RENAME TO b");  // renames
    list.add("DROP TABLOIDS b");            // fails
    list.add("ALTER TABLE b RENAME TO c");  // not executed
    withPool(pool ->
        pool.execute(list)
        .onComplete(context.asyncAssertFailure())
        .recover(x -> pool.execute(List.of("DROP TABLE b")))) // better now
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testSSL(TestContext context) throws IOException {
    Assume.assumeThat(System.getenv("DB_HOST"), is(nullValue()));
    Assume.assumeThat(System.getenv("DB_PORT"), is(nullValue()));
    configure("ssl=on");
    TenantPgPool.setMaxPoolSize("3");
    TenantPgPool.setServerPem(new String(TenantPgPoolTest.class.getClassLoader()
        .getResourceAsStream("server.crt").readAllBytes()));

    withPool(pool -> pool
        .query("SELECT version FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .execute())
    .onComplete(context.asyncAssertSuccess(rowSet -> {
      assertThat(rowSet.iterator().next().getString(0), is("TLSv1.3"));
    }));
  }

}
