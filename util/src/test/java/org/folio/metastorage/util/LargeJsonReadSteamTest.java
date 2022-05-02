package org.folio.metastorage.util;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@RunWith(VertxUnitRunner.class)
public class LargeJsonReadSteamTest {
  Vertx vertx;
  private final static Logger log = LogManager.getLogger(LargeJsonReadSteamTest.class.getName());

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void after(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }


  @Test
  public void parse(TestContext context){
    List<JsonObject> topLevel = new LinkedList<>();
    List<JsonObject> objects = new LinkedList<>();
    AtomicInteger total = new AtomicInteger();
    vertx.fileSystem().open("records-in.json", new OpenOptions())
        .compose(asyncFile -> {
          Promise<Void> p = Promise.promise();
          LargeJsonReadStream jors = new LargeJsonReadStream(asyncFile);
          jors
              .handler(jo -> {
                objects.add(jo);
              })
              .endHandler(x -> {
                total.set(jors.totalCount());
                topLevel.add(jors.topLevelObject());
                asyncFile.close();
                p.complete();
              })
              .exceptionHandler(e -> {
                asyncFile.close();
                p.fail(e);
              });
          return p.future();
        })
        .onComplete(context.asyncAssertSuccess(x ->{
          context.assertEquals(10, total.get());
          context.assertEquals(10, objects.size());
          context.assertEquals("d0166b80-1587-433c-b909-40ccbb1449f6",
              topLevel.get(0).getString("sourceId"));
          for (int i = 0; i < objects.size(); i++) {
            context.assertEquals("a"+(i+1), objects.get(i).getString("localId"));
          }
        }));
  }
}
