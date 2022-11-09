package org.folio.reservoir.server;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(VertxUnitRunner.class)
public class IngestWriteStreamTest {

  static Vertx vertx;
  @BeforeClass
  public static void beforeClass() {
    vertx = Vertx.vertx();
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testgetLocalIdFromMarc() {
    assertThat(IngestWriteStream.getLocalIdFromMarc(null), nullValue());
    assertThat(IngestWriteStream.getLocalIdFromMarc(new JsonObject()), nullValue());
    assertThat(IngestWriteStream.getLocalIdFromMarc(new JsonObject().put("fields", new JsonArray())), nullValue());
    Assert.assertThrows(ClassCastException.class,
        () -> IngestWriteStream.getLocalIdFromMarc(new JsonObject().put("fields", "2")));
    Assert.assertThrows(ClassCastException.class,
        () -> IngestWriteStream.getLocalIdFromMarc(new JsonObject().put("fields", new JsonArray().add("1"))));
    assertThat(IngestWriteStream.getLocalIdFromMarc(
        new JsonObject().put("fields", new JsonArray().add(new JsonObject().put("002", "3"))))
        , nullValue());
    assertThat(IngestWriteStream.getLocalIdFromMarc(
            new JsonObject().put("fields", new JsonArray().add(new JsonObject().put("001", null))))
        , nullValue());
    assertThat(IngestWriteStream.getLocalIdFromMarc(
            new JsonObject().put("fields", new JsonArray().add(new JsonObject().put("001", "1234"))))
        , is("1234"));
  }

}
