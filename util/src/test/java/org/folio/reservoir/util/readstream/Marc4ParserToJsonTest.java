package org.folio.reservoir.util.readstream;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class Marc4ParserToJsonTest {
  Vertx vertx;
  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void after(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  Future<Marc4ParserToJson> marc4ParserToXmlFromFile(String fname) {
    return vertx.fileSystem().open(fname, new OpenOptions())
        .map(Marc4ParserToJson::new);
  }

  Future<Marc4ParserToJson> marc4ParserToXmlFromFile() {
    return marc4ParserToXmlFromFile("marc3.marc");
  }

  @Test
  public void marc3(TestContext context) {
    marc4ParserToXmlFromFile()
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
          assertThat(records.get(0).getJsonArray("fields").getJsonObject(0).getString("001"), is("   73209622 //r823"));
          assertThat(records.get(1).getJsonArray("fields").getJsonObject(0).getString("001"), is("   11224466 "));
          assertThat(records.get(2).getJsonArray("fields").getJsonObject(0).getString("001"), is("   77123332 "));
        }));
  }

  @Test
  public void testEndHandler(TestContext context) {
    marc4ParserToXmlFromFile()
        .compose(parser -> {
          Promise<Void> promise = Promise.promise();
          parser.exceptionHandler(promise::tryFail);
          parser.endHandler(x -> promise.tryComplete());
          return promise.future();
        })
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testEndHandlerExceptionWithExceptionHandler(TestContext context) {
    marc4ParserToXmlFromFile()
        .compose(parser -> {
          Promise<Void> promise = Promise.promise();
          parser.exceptionHandler(promise::tryFail);
          parser.endHandler(x -> {
            throw new RuntimeException("end exception");
          });
          return promise.future();
        })
        .onComplete(context.asyncAssertFailure(e -> assertThat(e.getMessage(), is("end exception"))));
  }

  @Test
  public void testEndHandlerExceptionNoExceptionHandler(TestContext context) {
    marc4ParserToXmlFromFile()
        .compose(parser -> {
          Promise<Void> promise = Promise.promise();
          parser.endHandler(x -> {
            promise.tryFail("must stop");
            throw new RuntimeException("end exception");
          });
          return promise.future();
        })
        .onComplete(context.asyncAssertFailure(e -> assertThat(e.getMessage(), is("must stop"))));
  }

  @Test
  public void testHandler(TestContext context) {
    marc4ParserToXmlFromFile()
        .compose(parser -> {
          Promise<Void> promise = Promise.promise();
          AtomicInteger cnt = new AtomicInteger();
          parser.exceptionHandler(promise::tryFail);
          parser.handler(x -> {
            if (cnt.incrementAndGet() == 3) {
              promise.tryComplete();
            }
          });
          return promise.future();
        })
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testPauseFetch(TestContext context) {
    marc4ParserToXmlFromFile()
        .compose(parser -> {
          parser.fetch(1);
          parser.pause();
          parser.fetch(1);
          Promise<Void> promise = Promise.promise();
          AtomicInteger cnt = new AtomicInteger();
          parser.exceptionHandler(promise::tryFail);
          parser.endHandler(promise::tryComplete);
          parser.handler(x -> {
            if (cnt.incrementAndGet() == 1) {
              parser.pause();
              vertx.setTimer(10, p -> parser.resume());
            }
          });
          return promise.future();
        })
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void exceptionInHandler(TestContext context) {
    marc4ParserToXmlFromFile().compose(parser -> {
          Promise<Void> promise = Promise.promise();
          parser.handler(r -> {
            throw new RuntimeException("handler exception");
          });
          parser.exceptionHandler(promise::tryFail);
          parser.endHandler(promise::complete);
          return promise.future();
        })
        .onComplete(context.asyncAssertFailure(
            e -> assertThat(e.getMessage(), is("handler exception"))));
  }

  @Test
  public void testBadMarc(TestContext context) {
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("x00025" + "9".repeat(20)), vertx);
    Marc4jParser parser = new Marc4jParser(rs);
    Promise<Void> promise = Promise.promise();
    parser.exceptionHandler(promise::tryFail);
    parser.endHandler(x -> promise.complete());
    rs.run();
    promise.future()
        .onComplete(context.asyncAssertFailure(
            e -> assertThat(e.getMessage(), is("Index -1 out of bounds for length 0"))));

  }

  @Test
  public void testBadMarcNoExceptionHandler(TestContext context) {
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("x00025" + "9".repeat(20)), vertx);
    Marc4jParser parser = new Marc4jParser(rs);
    Promise<Void> promise = Promise.promise();
    parser.endHandler(x -> promise.complete());
    rs.run();
    promise.future()
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testBadMarc2(TestContext context) {
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("x00024"), Buffer.buffer("9"), 19, vertx);
    Marc4jParser parser = new Marc4jParser(rs);
    Promise<Void> promise = Promise.promise();
    parser.exceptionHandler(promise::tryFail);
    parser.endHandler(x -> promise.complete());
    rs.run();
    promise.future()
        .onComplete(context.asyncAssertFailure(
            e -> assertThat(e.getMessage(), is("Premature end of file encountered"))));
  }

  @Test
  public void testSkipLength(TestContext context) {
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("x00025"), Buffer.buffer("9"), 19, vertx);
    Marc4jParser parser = new Marc4jParser(rs);
    Promise<Void> promise = Promise.promise();
    parser.exceptionHandler(promise::tryFail);
    parser.endHandler(x -> promise.complete());
    rs.run();
    promise.future()
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testSkipLead(TestContext context) {
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("!" + "x".repeat(24)), vertx);
    Marc4jParser parser = new Marc4jParser(rs);
    Promise<Void> promise = Promise.promise();
    parser.exceptionHandler(promise::tryFail);
    parser.endHandler(x -> promise.complete());
    rs.run();
    promise.future()
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testAllLeadBad(TestContext context) {
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("!".repeat(5) + "9".repeat(23)), vertx);
    Marc4jParser parser = new Marc4jParser(rs);
    Promise<Void> promise = Promise.promise();
    parser.exceptionHandler(promise::tryFail);
    parser.endHandler(x -> promise.complete());
    rs.run();
    promise.future()
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testExceptionInStream(TestContext context) {
    MemoryReadStream rs = new MemoryReadStream(null, vertx);
    Marc4jParser parser = new Marc4jParser(rs);
    Promise<Void> promise = Promise.promise();
    parser.exceptionHandler(promise::tryFail);
    parser.endHandler(x -> promise.complete());
    rs.run();
    promise.future()
        .onComplete(context.asyncAssertFailure(
            e -> assertThat(e.getMessage(), is("Cannot read field \"buffer\" because \"impl\" is null"))));
  }

  @Test
  public void testExceptionInStreamNoExceptionHandler(TestContext context) {
    MemoryReadStream rs = new MemoryReadStream(null, vertx);
    Marc4jParser parser = new Marc4jParser(rs);
    Promise<Void> promise = Promise.promise();
    parser.endHandler(x -> promise.complete());
    rs.run();
    promise.future()
        .onComplete(context.asyncAssertSuccess());
  }
}
