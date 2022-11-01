package org.folio.reservoir.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.marc4j.marc.Record;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class Marc4jParserTest {
  Vertx vertx;
  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void after(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  Future<Marc4jParser> marc4jParserFromFile(String fname) {
    return vertx.fileSystem().open(fname, new OpenOptions())
        .map(asyncFile -> new Marc4jParser(asyncFile));
  }

  Future<List<Record>> eventsFromFile(String fname) {
    List<Record> events = new ArrayList<>();
    return marc4jParserFromFile(fname).compose(xmlParser -> {
      Promise<List<Record>> promise = Promise.promise();
      xmlParser.handler(events::add);
      xmlParser.endHandler(e -> promise.complete(events));
      xmlParser.exceptionHandler(e -> promise.tryFail(e));
      return promise.future();
    });
  }

  @Test
  public void marc3(TestContext context) {
    eventsFromFile("marc3.marc").onComplete(context.asyncAssertSuccess(records -> {
      assertThat(records, hasSize(3));
      assertThat(records.get(0).getControlNumber(), is("   73209622 //r823"));
      assertThat(records.get(1).getControlNumber(), is("   11224466 "));
      assertThat(records.get(2).getControlNumber(), is("   77123332 "));
    }));
  }

  @Test
  public void exceptionInHandler(TestContext context) {
    marc4jParserFromFile("marc3.marc")
        .compose(xmlParser -> {
          Promise<Void> promise = Promise.promise();
          xmlParser.handler(r -> {
            throw new RuntimeException("handlerExcept");
          });
          xmlParser.exceptionHandler(promise::tryFail);
          xmlParser.endHandler(promise::complete);
          return promise.future();
        })
        .onComplete(context.asyncAssertFailure(
            e -> assertThat(e.getMessage(), is("handlerExcept"))));
  }

}
