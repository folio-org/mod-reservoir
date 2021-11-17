package org.folio.reshare.index;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

@RunWith(VertxUnitRunner.class)
public class BasicDbTest {

  private static PostgreSQLContainer<?> postgresSQLContainer;
  private static PgPool pgPool;
  private static Vertx vertx;

  @BeforeClass
  public static void setUpBeforeClass(TestContext context) {
    vertx = Vertx.vertx();
    postgresSQLContainer = new PostgreSQLContainer<>("postgres:12-alpine");
    postgresSQLContainer.start();
    PgConnectOptions pgConnectOptions = new PgConnectOptions();
    pgConnectOptions.setHost(postgresSQLContainer.getHost());
    pgConnectOptions.setPort(postgresSQLContainer.getFirstMappedPort());
    pgConnectOptions.setUser(postgresSQLContainer.getUsername());
    pgConnectOptions.setPassword(postgresSQLContainer.getPassword());
    pgConnectOptions.setDatabase(postgresSQLContainer.getDatabaseName());
    PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
    pgPool = PgPool.pool(vertx, pgConnectOptions, poolOptions);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    postgresSQLContainer.close();
  }

  @Test
  public void createTable(TestContext context) {
    UUID id = UUID.randomUUID();
    Future<Void> future = Future.succeededFuture();

    future = future.compose(res ->
        pgPool.query("CREATE TABLE IF NOT EXISTS foo ( id UUID PRIMARY KEY, jsonb JSONB NOT NULL)")
            .execute().mapEmpty());
    future = future.compose(res ->
        pgPool.preparedQuery("SELECT * FROM foo WHERE id = $1")
            .execute(Tuple.of(id))
            .compose(res1 -> {
              context.assertEquals(0, res1.rowCount());
              return Future.succeededFuture();
            }));
    future = future.compose(res ->
        pgPool.preparedQuery("INSERT INTO foo (id, jsonb) VALUES ($1, $2)")
            .execute(Tuple.of(id, new JsonObject().put("a", "b"))).mapEmpty());
    future = future.compose(res ->
        pgPool.preparedQuery("SELECT * FROM foo WHERE id = $1")
            .execute(Tuple.of(id))
            .compose(res1 -> {
              context.assertEquals(1, res1.rowCount());
              return Future.succeededFuture();
            }));
    future = future.compose(res ->
        pgPool.query("DROP TABLE foo")
            .execute().mapEmpty());
    future.onComplete(context.asyncAssertSuccess());
  }

}
