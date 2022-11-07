package org.folio.reservoir.util;

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
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MarcXmlParserToJsonTest {
  Vertx vertx;

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void after(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  Future<MarcXmlParserToJson> marcXmlParserFromFile(String fname) {
    return vertx.fileSystem().open(fname, new OpenOptions())
        .map(asyncFile -> new MarcXmlParserToJson(XmlParser.newParser(asyncFile)));
  }

  Future<List<JsonObject>> getRecordsFromFile(String fname) {
    List<JsonObject> records = new ArrayList<>();
    return marcXmlParserFromFile(fname).compose(parser -> {
      Promise<List<JsonObject>> promise = Promise.promise();
      parser.handler(records::add);
      parser.endHandler(e -> promise.complete(records));
      parser.exceptionHandler(e -> promise.tryFail(e));
      return promise.future();
    });
  }

  @Test
  public void marc3(TestContext context) {
    getRecordsFromFile("marc3.xml")
        .onComplete(context.asyncAssertSuccess(records -> {
          assertThat(records.size(), is(3));
          assertThat(records.get(0).getJsonArray("fields").getJsonObject(0).getString("001"), is("   73209622 //r823"));
          assertThat(records.get(1).getJsonArray("fields").getJsonObject(0).getString("001"), is("   11224466 "));
          assertThat(records.get(2).getJsonArray("fields").getJsonObject(0).getString("001"), is("   77123332 "));
        }));
  }
}
