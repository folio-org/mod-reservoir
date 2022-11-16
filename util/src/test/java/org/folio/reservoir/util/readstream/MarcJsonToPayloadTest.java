package org.folio.reservoir.util.readstream;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@RunWith(VertxUnitRunner.class)
public class MarcJsonToPayloadTest {
  Vertx vertx;

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void after(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  Future<MarcJsonToPayload> marcFromFile(String fname) {
    return vertx.fileSystem().open(fname, new OpenOptions())
        .map(x -> new MarcJsonToPayload(new MarcToJsonParser(x)));
  }

  String get001(JsonObject marc) {
    return marc.getJsonArray("fields").getJsonObject(0).getString("001");
  }

  @Test
  public void marc3(TestContext context) {
    marcFromFile("marc3.marc")
        .compose(parser -> {
          List<JsonObject> records = new ArrayList<>();
          Promise<List<JsonObject>> promise = Promise.promise();
          parser.handler(records::add);
          parser.endHandler(e -> promise.complete(records));
          parser.exceptionHandler(promise::tryFail);
          return promise.future();
        })
        .onComplete(context.asyncAssertSuccess(records -> {
          assertThat(records, hasSize(3));
          assertThat(get001(records.get(0).getJsonObject("marc")), is("   73209622 //r823"));
          assertThat(records.get(0).containsKey("marcHoldings"), is(false));
          assertThat(get001(records.get(1).getJsonObject("marc")), is("   11224466 "));
          assertThat(get001(records.get(2).getJsonObject("marc")), is("   77123332 "));
        }));
  }

  @Test
  public void mfhd(TestContext context) {
    marcFromFile("mfhd.marc")
        .compose(parser -> {
          List<JsonObject> records = new ArrayList<>();
          Promise<List<JsonObject>> promise = Promise.promise();
          parser.handler(records::add);
          parser.endHandler(e -> promise.complete(records));
          parser.exceptionHandler(promise::tryFail);
          return promise.future();
        })
        .onComplete(context.asyncAssertSuccess(records -> {
          assertThat(records, hasSize(7));
          assertThat(get001(records.get(0).getJsonObject("marc")), is("ocm22544415 "));
          assertThat(get001(records.get(0).getJsonArray("marcHoldings").getJsonObject(0)), is("203780362"));
          assertThat(get001(records.get(6).getJsonObject("marc")), is("on1328230635"));
          assertThat(get001(records.get(6).getJsonArray("marcHoldings").getJsonObject(0)), is("385266588"));
        }));
  }

  @Test
  public void parentMarcIsHolding(TestContext context) {
    marcFromFile("mfhd-no-parent.marc")
        .compose(parser -> {
          List<JsonObject> records = new ArrayList<>();
          Promise<List<JsonObject>> promise = Promise.promise();
          parser.handler(records::add);
          parser.endHandler(e -> promise.complete(records));
          parser.exceptionHandler(promise::tryFail);
          return promise.future();
        })
        .onComplete(context.asyncAssertFailure(
            e -> assertThat(e.getMessage(), startsWith("Parent MARC record is holding"))
        ));
  }

}
