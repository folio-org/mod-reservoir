package org.folio.reservoir.solr.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.folio.reservoir.solr.VertxSolrClient;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class VertxSolrClientTest {
  private static Vertx vertx;

  private static int mockPort = 9230;

  private static String solrUrl = "http://localhost:8983/solr";
  private static String mockUrl = "http://localhost:" + mockPort + "/solr";

  private final static String COLLECTION = "col1";

  private static WebClient webClient;

  private static boolean hasSolr;

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
    webClient = WebClient.create(vertx);
    checkSolr()
        .onSuccess(x -> hasSolr = true)
        .recover(x -> Future.succeededFuture())
        .compose(x -> setupMockSolr())
        .onComplete(context.asyncAssertSuccess());
  }

  static Future<Void> checkSolr() {
    return webClient.getAbs(solrUrl + "/" + COLLECTION + "/select")
        .addQueryParam("q", "title:title1")
        .addQueryParam("wt", "json")
        .send()
        .map(res -> {
          JsonObject ret = res.bodyAsJsonObject();
          if (!ret.containsKey("responseHeader")) {
            throw new DecodeException("Does not have responseHeader");
          }
          return null;
        });
  }

  static Future<Void> setupMockSolr() {
    Router router = Router.router(vertx);
    router.post().handler(BodyHandler.create());
    router.get("/solr/" + COLLECTION + "/select").handler(c -> {
      System.out.println("get path=" + c.request().path());
      JsonObject response = new JsonObject()
          .put("responseHeader",
              new JsonObject()
                  .put("status", 0)
          )
          .put("response", new JsonObject()
              .put("numFound", 1)
          );
      c.response().setStatusCode(200);
      c.response().putHeader("Content-Type", "application/json");
      c.response().end(response.encodePrettily());
    });
    router.post().handler(c -> {
      System.out.println("post path=" + c.request().path());
      c.request().headers().forEach((n, v) -> System.out.println(n + "=" + v));
      JsonObject response = new JsonObject()
          .put("responseHeader",
              new JsonObject()
                  .put("status", 0)
          );
      c.response().setStatusCode(200);
      c.response().putHeader("Content-Type", "application/json");
      c.response().end(response.encodePrettily());
    });
    HttpServer httpServer = vertx.createHttpServer().requestHandler(router);
    return httpServer.listen(mockPort).mapEmpty();
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void addOneDocument(TestContext context) {
    Assume.assumeTrue(hasSolr);
    final UUID docId = UUID.randomUUID();

    VertxSolrClientSolrj c = new VertxSolrClientSolrj(vertx, solrUrl, COLLECTION);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", docId.toString());
    doc.addField("title", "title1");
    Collection<SolrInputDocument> docs = List.of(doc);
    c.add(docs)
        .compose(x -> c.commit())
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void queryOneDocument(TestContext context) {
    Assume.assumeTrue(hasSolr);
    final UUID docId = UUID.randomUUID();
    VertxSolrClientSolrj c = new VertxSolrClientSolrj(vertx, solrUrl, COLLECTION);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", docId.toString());
    doc.addField("title", List.of("title2a", "title2b"));
    Collection<SolrInputDocument> docs = List.of(doc);
    Map<String,String> map = Map.of("q", "id:" + docId, "fl", "id,title", "sort", "id asc");
    MapSolrParams params = new MapSolrParams(map);
    c.add(docs)
        .compose(x -> c.commit())
        .compose(x -> c.query(params))
        .onComplete(context.asyncAssertSuccess(res -> {
          SolrDocumentList results = res.getResults();
          assertThat(results.getNumFound(), is(1L));
          assertThat(results.get(0).get("id"), is(docId.toString()));
          assertThat(results.get(0).get("title"), is(List.of("title2a", "title2b")));
        }));
  }

  @Test
  public void addOneJsonDocumentSolrj(TestContext context) {
    Assume.assumeTrue(hasSolr);
    final UUID docId = UUID.randomUUID();
    VertxSolrClient c = new VertxSolrClientSolrj(vertx, solrUrl, COLLECTION);
    JsonArray docs = new JsonArray()
        .add(new JsonObject()
            .put("id", docId.toString())
            .put("title", new JsonArray().add("title3a").add("title3b")));
    Map<String,String> map = Map.of("q", "id:" + docId, "fl", "id,title", "sort", "id asc");
    c.add(docs)
        .compose(x -> c.commit())
        .compose(x -> c.query(map))
        .onComplete(context.asyncAssertSuccess(res -> {
          assertThat(res.getJsonObject("response").getLong("numFound"), is(1L));
        }));
  }

  @Test
  public void commitSolrj(TestContext context) {
    Assume.assumeTrue(hasSolr);
    VertxSolrClient c = new VertxSolrClientSolrj(vertx, solrUrl, COLLECTION);
    c.commit()
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void searchWebClient(TestContext context) {
    Assume.assumeTrue(hasSolr);
    final UUID docId = UUID.randomUUID();
    VertxSolrClient c = VertxSolrClient.create(vertx, solrUrl, COLLECTION);
    JsonArray docs = new JsonArray()
        .add(new JsonObject()
            .put("id", docId.toString())
            .put("title", new JsonArray().add("title3a").add("title3b")));
    Map<String,String> map = Map.of("q", "id:" + docId, "fl", "id,title", "sort", "id asc");
    c.add(docs)
        .compose(x -> c.commit())
        .compose(x -> c.query(map))
        .onComplete(context.asyncAssertSuccess(res -> {
          assertThat(res.getJsonObject("response").getLong("numFound"), is(1L));
        }));
  }

  @Test
  public void searchWebClientMock(TestContext context) {
    final UUID docId = UUID.randomUUID();
    VertxSolrClient c = VertxSolrClient.create(vertx, mockUrl, COLLECTION);
    JsonArray docs = new JsonArray()
        .add(new JsonObject()
            .put("id", docId.toString())
            .put("title", new JsonArray().add("title3a").add("title3b")));
    Map<String,String> map = Map.of("q", "id:" + docId, "fl", "id,title", "sort", "id asc");
    c.add(docs)
        .compose(x -> c.commit())
        .compose(x -> c.query(map))
        .onComplete(context.asyncAssertSuccess(res -> {
          assertThat(res.getJsonObject("response").getLong("numFound"), is(1L));
        }));
  }

}
