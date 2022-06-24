package org.folio.metastorage.module.impl;

import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.UUID;
import org.folio.metastorage.module.ModuleCache;
import org.folio.metastorage.server.entity.ClusterBuilder;
import org.graalvm.polyglot.PolyglotException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class ModuleTest {

  static Vertx vertx;

  static int PORT = 9231;

  static String HOSTPORT = "http://localhost:" + PORT;

  static String TENANT = "test";

  @BeforeClass
  public static void beforeClass(TestContext context)  {
    vertx = Vertx.vertx();
    ModuleScripts.serveModules(vertx, PORT).onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  @After
  public void before() {
    ModuleCache.getInstance().purgeAll();
  }

  JsonArray recordsIn = new JsonArray()
      //first record
      .add(new JsonObject()
          .put("globalId", "source-1-record-1")
          .put("localId", "REC:A")
          .put("sourceId", "source-1")
          .put("payload", new JsonObject()
              .put("marc", new JsonObject()
                  .put("leader", "leader-1")
                  .put("fields", new JsonArray()
                      .add(new JsonObject()
                          .put("245", new JsonObject()
                              .put("subfields", new JsonArray()
                                  .add(new JsonObject().put("a", "source-1 title"))
                              )
                          )
                      )
                      .add(new JsonObject()
                          .put("998", new JsonObject()
                              .put("subfields", new JsonArray()
                                  .add(new JsonObject().put("x", "source-1 location"))
                              )
                          )
                      )
                  )
              )
          )
      )
      //second record
      .add(new JsonObject()
          .put("globalId", "source-2-record-2")
          .put("localId", "rec_1")
          .put("sourceId", "source-2")
          .put("payload", new JsonObject()
              .put("marc", new JsonObject()
                  .put("leader", "leader-1")
                  .put("fields", new JsonArray()
                      .add(new JsonObject()
                          .put("245", new JsonObject()
                              .put("subfields", new JsonArray()
                                  .add(new JsonObject().put("a", "source-2 title"))
                              )
                          )
                      )
                      .add(new JsonObject()
                          .put("998", new JsonObject()
                              .put("subfields", new JsonArray()
                                  .add(new JsonObject().put("x", "source-2 location"))
                              )
                          )
                      )
                  )
              )
          )
      );

  @Test
  public void testIsbnTransformerUrl(TestContext context) {
      JsonObject recordOut = new JsonObject()
      //merged record
        .put("leader", "new leader")
        .put("fields", new JsonArray()
          .add(new JsonObject()
            .put("245", new JsonObject()
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("a", "source-1 title"))
              )
            )
          )
          .add(new JsonObject()
            .put("998", new JsonObject()
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("x", "source-1 location"))
              )
            )
          )
          .add(new JsonObject()
            .put("999", new JsonObject()
              .put("ind1", "1")
              .put("ind2", "0")
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("i", "source-1-record-1"))
                .add(new JsonObject().put("l", "REC:A"))
                .add(new JsonObject().put("s", "source-1"))
              )
            )
          )
          .add(new JsonObject()
            .put("245", new JsonObject()
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("a", "source-2 title"))
              )
            )
          )
          .add(new JsonObject()
            .put("998", new JsonObject()
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("x", "source-2 location"))
              )
            )
          )
          .add(new JsonObject()
            .put("999", new JsonObject()
              .put("ind1", "1")
              .put("ind2", "0")
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("i", "source-2-record-2"))
                .add(new JsonObject().put("l", "rec_1"))
                .add(new JsonObject().put("s", "source-2"))
              )
            )
          )
        );

    ClusterBuilder cb = new ClusterBuilder(UUID.randomUUID());
    cb.records(recordsIn);
    JsonObject input = cb.build();

    JsonObject config = new JsonObject()
      .put("id", "marc-transformer")
      .put("url", HOSTPORT + "/lib/marc-transformer.mjs")
      .put("function", "transform");

    ModuleCache.getInstance().lookup(vertx, TENANT, config)
      .compose(m -> m.execute(input).eventually(x -> m.terminate()))
      .onComplete(context.asyncAssertSuccess(output -> context.assertEquals(recordOut, output))
    );
  }

  @Test
  public void moduleReturnInt(TestContext context) {
    ClusterBuilder cb = new ClusterBuilder(UUID.randomUUID());
    cb.records(recordsIn);
    JsonObject input = cb.build();

    JsonObject config = new JsonObject()
        .put("id", "returns-int")
        .put("url", HOSTPORT + "/lib/returns-int.mjs")
        .put("function", "transform");

    ModuleCache.getInstance().lookup(vertx, TENANT, config)
        .compose(m -> m.execute(input).eventually(x -> m.terminate()))
        .onComplete(context.asyncAssertFailure(
            e -> assertThat(e.getMessage(), containsString("must return JSON string"))));
  }

  @Test
  public void moduleThrowsException(TestContext context) {
    ClusterBuilder cb = new ClusterBuilder(UUID.randomUUID());
    cb.records(recordsIn);
    JsonObject input = cb.build();

    JsonObject config = new JsonObject()
        .put("id", "throw")
        .put("url", HOSTPORT + "/lib/throw.mjs")
        .put("function", "transform");

    ModuleCache.getInstance().lookup(vertx, TENANT, config)
        .compose(m -> m.execute(input).eventually(x -> m.terminate()))
        .onComplete(context.asyncAssertFailure(
            e -> {
              assertThat(e.getClass(), is(PolyglotException.class));
              assertThat(e.getMessage(), is("Error"));
            }));
  }

  @Test
  public void moduleBadJson(TestContext context) {
    ClusterBuilder cb = new ClusterBuilder(UUID.randomUUID());
    cb.records(recordsIn);
    JsonObject input = cb.build();

    JsonObject config = new JsonObject()
        .put("id", "bad-json")
        .put("url", HOSTPORT + "/lib/bad-json.mjs")
        .put("function", "transform");

    ModuleCache.getInstance().lookup(vertx, TENANT, config)
        .compose(m -> m.execute(input).eventually(x -> m.terminate()))
        .onComplete(context.asyncAssertFailure(
            e -> {
              assertThat(e.getClass(), is(DecodeException.class));
              assertThat(e.getMessage(), containsString("Unexpected end-of-input"));
            }));
  }

  @Test
  public void cachingEquals(TestContext context) {
    JsonObject config = new JsonObject()
        .put("id", "marc-transformer")
        .put("url", HOSTPORT + "/lib/marc-transformer.mjs")
        .put("function", "transform");
    ModuleCache.getInstance().lookup(vertx, TENANT, config).compose(m1 ->
        ModuleCache.getInstance().lookup(vertx, TENANT, config).map(m2 -> m1 == m2))
    .onComplete(context.asyncAssertSuccess(equals -> context.assertTrue(equals)));
  }

  @Test
  public void cachingPurge(TestContext context) {
    JsonObject config = new JsonObject()
        .put("id", "marc-transformer")
        .put("url", HOSTPORT + "/lib/marc-transformer.mjs")
        .put("function", "transform");
    ModuleCache.getInstance().lookup(vertx, TENANT, config).compose(m1 -> {
      ModuleCache.getInstance().purge(TENANT, "marc-transformer");
      ModuleCache.getInstance().purge(TENANT, "marc-transformer");
      return ModuleCache.getInstance().lookup(vertx, TENANT, config).map(m2 -> m1 == m2);
    })
    .onComplete(context.asyncAssertSuccess(equals -> context.assertFalse(equals)));
  }

  @Test
  public void cachingNotEquals(TestContext context) {
    JsonObject config = new JsonObject()
        .put("id", "marc-transformer1")
        .put("url", HOSTPORT + "/lib/marc-transformer.mjs")
        .put("function", "transform");

    ModuleCache.getInstance().lookup(vertx, TENANT, config).compose(m1 -> {
      config.put("id", "marc-transformer2");
      return ModuleCache.getInstance().lookup(vertx, TENANT, config).map(m2 -> m1 == m2);
    })
    .onComplete(context.asyncAssertSuccess(equals -> context.assertFalse(equals)));
  }

  @Test
  public void cachingChangedConfig(TestContext context) {
    JsonObject config1 = new JsonObject()
        .put("id", "marc-transformer")
        .put("url", HOSTPORT + "/lib/marc-transformer.mjs")
        .put("function", "transform");
    JsonObject config2 = new JsonObject()
        .put("id", "marc-transformer")
        .put("url", HOSTPORT + "/lib/marc-transformer.mjs")
        .put("foo", "bar")
        .put("function", "transform");
    ModuleCache.getInstance().lookup(vertx, TENANT, config1).compose(m1 ->
      ModuleCache.getInstance().lookup(vertx, TENANT, config2).map(m2 -> m1 == m2))
    .onComplete(context.asyncAssertSuccess(equals -> context.assertFalse(equals)));
  }

  @Test
  public void exceptionInInitialize(TestContext context) {
    JsonObject config1 = new JsonObject()
        .put("id", "marc-transformer")
        .put("url", HOSTPORT + "/lib/marc-transformer.mjs")
        .put("function", "transform1");
    ModuleCache.getInstance().lookup(vertx, TENANT, config1)
        .onComplete(context.asyncAssertFailure(e ->
            assertThat(e.getMessage(), containsString("Invariant contract violation"))));
  }

  @Test
  public void mustEndInMjs(TestContext context) {
    JsonObject config1 = new JsonObject()
        .put("id", "marc-transformer")
        .put("url", HOSTPORT + "/lib/marc-transformer.js")
        .put("function", "transform1");
    ModuleCache.getInstance().lookup(vertx, TENANT, config1)
        .onComplete(context.asyncAssertFailure(e ->
            assertThat(e.getMessage(), is("url must end with .mjs to designate ES module"))));
  }

  @Test
  public void missingUrl(TestContext context) {
    JsonObject config1 = new JsonObject()
        .put("id", "marc-transformer")
        .put("function", "transform");
    ModuleCache.getInstance().lookup(vertx, TENANT, config1)
        .onComplete(context.asyncAssertFailure(e ->
            assertThat(e.getMessage(), is("Module config must include 'url'"))));
  }

  @Test
  public void missingFunction(TestContext context) {
    JsonObject config1 = new JsonObject()
        .put("id", "marc-transformer")
        .put("url", HOSTPORT + "/lib/marc-transformer.mjs");
    ModuleCache.getInstance().lookup(vertx, TENANT, config1)
        .onComplete(context.asyncAssertFailure(e ->
            assertThat(e.getMessage(), is("Module config must include 'function'"))));
  }

  @Test
  public void missingModuleId(TestContext context) {
    JsonObject config1 = new JsonObject()
        .put("url", HOSTPORT + "/lib/marc-transformer.mjs")
        .put("function", "transform");
    ModuleCache.getInstance().lookup(vertx, TENANT, config1)
        .onComplete(context.asyncAssertFailure(e ->
            assertThat(e.getMessage(), is("module config must include 'id'"))));
  }


}
