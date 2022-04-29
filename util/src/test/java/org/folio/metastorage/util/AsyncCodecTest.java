package org.folio.metastorage.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class AsyncCodecTest {
  Vertx vertx;

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void before(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }


  @Test
  public void compressAndDecompress(TestContext context){
    String s = "compression test";
    Future<Buffer> f = AsyncCodec.compress(vertx, Buffer.buffer(s));
    f = f.compose(bc -> {
        return AsyncCodec.decompress(vertx, bc);
    });
    f.onComplete(context.asyncAssertSuccess(bd -> {
      context.assertEquals(s, bd.toString());
    }));
  }

}
