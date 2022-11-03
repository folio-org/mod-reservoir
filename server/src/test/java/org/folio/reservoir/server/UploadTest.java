package org.folio.reservoir.server;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.multipart.MultipartForm;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class UploadTest extends TestBase {

  Buffer marc3marcBuffer;

  Buffer marc3xmlBuffer;

  @Before
  public void before(TestContext context) {
    FileSystem fileSystem = vertx.fileSystem();
    fileSystem
        .readFile("src/test/resources/marc3.marc")
        .onSuccess(x -> marc3marcBuffer = x)
        .compose(x -> fileSystem.readFile("src/test/resources/marc3.xml"))
        .onSuccess(x -> marc3xmlBuffer = x)
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void uploadOctetStream(TestContext context) {
    WebClient webClient = WebClient.create(vertx);

    MultipartForm body = MultipartForm.create()
        .binaryFileUpload("records", "tiny.mrc", Buffer.buffer("01234"), "application/octet-stream");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload")
        .expect(ResponsePredicate.SC_OK)
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .addQueryParam("sourceId", "SOURCE-1")
        .addQueryParam("sourceVersion", "1")
        .addQueryParam("localIdPath", "path")
        .sendMultipartForm(body)
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void uploadIso2709WithIngest(TestContext context) {
    MultipartForm requestForm = MultipartForm.create()
        .binaryFileUpload("records", "marc3.mrc", marc3marcBuffer,  "application/marc");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload")
        .expect(ResponsePredicate.SC_OK)
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .addQueryParam("sourceId", "SOURCE-1")
        .addQueryParam("sourceVersion", "1")
        .sendMultipartForm(requestForm)
        .compose(c1 ->
            webClient.getAbs(OKAPI_URL + "/reservoir/records")
                .addQueryParam("query", "sourceId = \"SOURCE-1\"")
                .expect(ResponsePredicate.SC_OK)
                .putHeader(XOkapiHeaders.TENANT, TENANT_1)
                .send()
        )
        .map(res -> {
          JsonObject responseBody = res.bodyAsJsonObject();
          assertThat(responseBody.getJsonArray("items").size(), is(3));
          return null;
        })
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void uploadIso2709WithoutIngest(TestContext context) {
    MultipartForm requestForm = MultipartForm.create()
        .binaryFileUpload("records", "marc3.mrc", marc3marcBuffer,  "application/marc");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload")
        .expect(ResponsePredicate.SC_OK)
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .addQueryParam("sourceId", "SOURCE-1")
        .addQueryParam("sourceVersion", "1")
        .addQueryParam("ingest", "false")
        .sendMultipartForm(requestForm)
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void uploadMarcXml(TestContext context) {
    MultipartForm requestForm = MultipartForm.create()
        .binaryFileUpload("records", "marc3.xml", marc3xmlBuffer,  "text/xml");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload")
        .expect(ResponsePredicate.SC_OK)
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .addQueryParam("sourceId", "SOURCE-2")
        .addQueryParam("sourceVersion", "1")
        .addQueryParam("localIdPath",  "$.marc.fields[*].001")
        .sendMultipartForm(requestForm)
        .compose(c1 ->
            webClient.getAbs(OKAPI_URL + "/reservoir/records")
                .addQueryParam("query", "sourceId = \"SOURCE-2\"")
                .expect(ResponsePredicate.SC_OK)
                .putHeader(XOkapiHeaders.TENANT, TENANT_1)
                .send()
        )
        .map(res -> {
          JsonObject responseBody = res.bodyAsJsonObject();
          assertThat(responseBody.getJsonArray("items").size(), is(3));
          return null;
        })
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void uploadPdf(TestContext context) {
    WebClient webClient = WebClient.create(vertx);

    MultipartForm body = MultipartForm.create()
        .binaryFileUpload("records", "records.mrc", Buffer.buffer("0"),  "application/pdf");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload")
        .expect(ResponsePredicate.SC_BAD_REQUEST)
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .addQueryParam("sourceId", "SOURCE-1")
        .addQueryParam("sourceVersion", "1")
        .addQueryParam("localIdPath", "path")
        .sendMultipartForm(body)
        .onComplete(context.asyncAssertSuccess(res ->
            assertThat(res.bodyAsString(), containsString("Unsupported content-type: application/pdf"))
        ));
  }

  @Test
  public void uploadRaw(TestContext context) {
    MultipartForm requestForm = MultipartForm.create()
        .binaryFileUpload("records", "marc3.mrc", marc3marcBuffer,  "application/marc");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload")
        .expect(ResponsePredicate.SC_OK)
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .addQueryParam("sourceId", "SOURCE-1")
        .addQueryParam("sourceVersion", "1")
        .addQueryParam("localIdPath", "path")
        .addQueryParam("raw", "true")
        .sendMultipartForm(requestForm)
        .onComplete(context.asyncAssertSuccess());
  }

}
