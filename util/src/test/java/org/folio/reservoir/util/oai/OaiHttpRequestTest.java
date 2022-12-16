package org.folio.reservoir.util.oai;

import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import org.folio.reservoir.util.XmlMetadataParserMarcInJson;
import org.folio.reservoir.util.XmlMetadataStreamParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@RunWith(VertxUnitRunner.class)
public class OaiHttpRequestTest {

  static final int OKAPI_PORT = 9230;

  static final String OKAPI_URL = "http://localhost:" + OKAPI_PORT;

  static Vertx vertx;

  static HttpClient httpClient;

  static String oaiFilename = null;

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
    httpClient = vertx.createHttpClient();
    FileSystem fileSystem = vertx.fileSystem();
    Router router = Router.router(vertx);
    router.get("/oai").handler(ctx -> {
      HttpServerResponse response = ctx.response();
      response.setChunked(true);
      if (oaiFilename == null) {
        response.setStatusCode(400);
        response.end("User error");
        return;
      }
      fileSystem.open(oaiFilename, new OpenOptions())
          .compose(filestream -> {
            System.out.println("reading file " + oaiFilename);
            response.setStatusCode(200);
            response.putHeader(HttpHeaders.CONTENT_TYPE, "text/xml");
            return filestream.pipeTo(response);
          })
          .onFailure(e -> {
            System.out.println("file system failure");
            response.setStatusCode(500);
            response.putHeader(HttpHeaders.CONTENT_TYPE, "text/xml");
            response.end(e.getMessage());
          });
    });
    vertx.createHttpServer(new HttpServerOptions())
        .requestHandler(router)
        .listen(OKAPI_PORT)
        .onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }


  @Test
  public void testNotFound(TestContext context) {
    XmlMetadataStreamParser<JsonObject> metadataParser = new XmlMetadataParserMarcInJson();
    OaiRequest<JsonObject> oaiRequest = new OaiHttpRequest<>(
        httpClient, OKAPI_URL + "/xx", metadataParser, false, MultiMap.caseInsensitiveMultiMap());
    oaiRequest.listRecords().onComplete(context.asyncAssertFailure(oaiResponse -> {
      assertThat(oaiResponse.getMessage(), is("OAI server returned status 404"));
    }));
  }

  @Test
  public void test1(TestContext context) {
    XmlMetadataStreamParser<JsonObject> metadataParser = new XmlMetadataParserMarcInJson();
    oaiFilename = "oai-response-1.xml";
    OaiRequest<JsonObject> oaiRequest = new OaiHttpRequest<>(
        httpClient, OKAPI_URL + "/oai", metadataParser, false, MultiMap.caseInsensitiveMultiMap());
    oaiRequest.listRecords().onComplete(context.asyncAssertSuccess(oaiResponse -> {
      List<OaiRecord<JsonObject>> records = new ArrayList<>();
      oaiResponse.handler(records::add);
      Promise<Void> promise = Promise.promise();
      oaiResponse.endHandler(x -> promise.complete());
      oaiResponse.exceptionHandler(promise::tryFail);
      promise.future().onComplete(context.asyncAssertSuccess(check -> {
        assertThat(records, hasSize(4));
        assertThat(records.get(0).deleted, is(true));
        assertThat(records.get(1).deleted, is(false));
        assertThat(records.get(2).deleted, is(false));
        assertThat(records.get(3).deleted, is(false));
        assertThat(records.get(0).identifier, is("998212783503681"));
        assertThat(records.get(1).identifier, is("9977919382003681"));
        assertThat(records.get(2).identifier, is("9977924842403681"));
        assertThat(records.get(3).identifier, is("9977648149503681"));
        assertThat(records.get(0).metadata, nullValue());
        assertThat(records.get(1).metadata.getString("leader"), is("10873cam a22004693i 4500"));
        assertThat(records.get(2).metadata.getString("leader"), is("02052cam a22004213i 4500"));
        assertThat(records.get(3).metadata.getString("leader"), is("02225nam a2200469 i 4500"));
        assertThat(oaiResponse.resumptionToken(), is("MzM5OzE7Ozt2MS4w"));
        assertThat(records.get(3).datestamp, is("2022-05-03"));
      }));
    }));
  }

}
