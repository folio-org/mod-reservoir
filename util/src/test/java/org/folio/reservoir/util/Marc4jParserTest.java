package org.folio.reservoir.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
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
import java.util.concurrent.atomic.AtomicInteger;

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

  Future<List<Record>> getRecordsFromFile(String fname) {
    List<Record> records = new ArrayList<>();
    return marc4jParserFromFile(fname).compose(xmlParser -> {
      Promise<List<Record>> promise = Promise.promise();
      xmlParser.handler(records::add);
      xmlParser.endHandler(e -> promise.complete(records));
      xmlParser.exceptionHandler(e -> promise.tryFail(e));
      return promise.future();
    });
  }

  @Test
  public void marc3(TestContext context) {
    getRecordsFromFile("marc3.marc")
        .onComplete(context.asyncAssertSuccess(records -> {
          assertThat(records, hasSize(3));
          assertThat(records.get(0).getControlNumber(), is("   73209622 //r823"));
          assertThat(records.get(1).getControlNumber(), is("   11224466 "));
          assertThat(records.get(2).getControlNumber(), is("   77123332 "));
        }));
  }

  @Test
  public void testEndHandler(TestContext context) {
    marc4jParserFromFile("marc3.marc")
        .compose(parser -> {
          Promise<Void> promise = Promise.promise();
          parser.exceptionHandler(promise::tryFail);
          parser.endHandler(x -> {
            promise.tryComplete();
          });
          return promise.future();
        })
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testEndHandlerException(TestContext context) {
    marc4jParserFromFile("marc3.marc")
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
  public void testHandler(TestContext context) {
    marc4jParserFromFile("marc3.marc")
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
    marc4jParserFromFile("marc3.marc")
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
  public void testEmitted(TestContext context) {
    marc4jParserFromFile("marc3.marc")
        .compose(parser -> {
          Promise<Void> promise = Promise.promise();
          parser.exceptionHandler(promise::tryFail);
          parser.endHandler(promise::tryComplete);
          parser.handler(x -> parser.end());
          return promise.future();
        })
        .onComplete(context.asyncAssertFailure(
            e -> assertThat(e.getMessage(), is("Parsing already done"))));
  }

  @Test
  public void testDoubleEnd(TestContext context) {
    marc4jParserFromFile("marc3.marc")
        .map(parser -> {
          parser.end();
          parser.endHandler(x -> {});
          parser.end();
          return null;
        })
        .onComplete(context.asyncAssertFailure(
            e -> assertThat(e.getMessage(), is("Parsing already done"))));
  }

  @Test
  public void exceptionInHandler(TestContext context) {
    marc4jParserFromFile("marc3.marc").compose(parser -> {
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
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("x00025" + "9".repeat(20)), null, null, Buffer.buffer(), 0, vertx);
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
  public void testBadMarc2(TestContext context) {
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("x00024" + "9".repeat(19)), null, null, Buffer.buffer(), 0, vertx);
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
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("x00025" + "9".repeat(19)), null, null, Buffer.buffer(), 0, vertx);
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
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("!" + "x".repeat(24)), null, null, Buffer.buffer(), 0, vertx);
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
    MemoryReadStream rs = new MemoryReadStream(Buffer.buffer("!".repeat(5) + "9".repeat(20)), null, null, Buffer.buffer(), 0, vertx);
    Marc4jParser parser = new Marc4jParser(rs);
    Promise<Void> promise = Promise.promise();
    parser.exceptionHandler(promise::tryFail);
    parser.endHandler(x -> promise.complete());
    rs.run();
    promise.future()
        .onComplete(context.asyncAssertSuccess());
  }

}
