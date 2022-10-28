package org.folio.reservoir.server;

import io.vertx.core.buffer.Buffer;
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

@RunWith(VertxUnitRunner.class)
public class UploadTest extends TestBase {

  Buffer marc3Buffer;
  @Before
  public void before(TestContext context) {
        vertx.fileSystem().readFile("src/test/resources/marc3.marc")
        .onSuccess(x -> marc3Buffer = x)
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void uploadMimeTypeOctetStream(TestContext context) {
    WebClient webClient = WebClient.create(vertx);

    MultipartForm body = MultipartForm.create()
        .binaryFileUpload("records", "tiny.mrc", Buffer.buffer("01234"), "application/octet-stream");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload/records")
        .expect(ResponsePredicate.SC_OK)
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .addQueryParam("sourceId", "SOURCE-1")
        .addQueryParam("sourceVersion", "1")
        .addQueryParam("localIdPath", "path")
        .sendMultipartForm(body)
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void uploadMimeTypeApplicationMarc(TestContext context) {
    WebClient webClient = WebClient.create(vertx);

    MultipartForm body = MultipartForm.create()
        .binaryFileUpload("records", "marc3.mrc", marc3Buffer,  "application/marc");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload/records")
        .expect(ResponsePredicate.SC_OK)
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .addQueryParam("sourceId", "SOURCE-1")
        .addQueryParam("sourceVersion", "1")
        .addQueryParam("localIdPath", "path")
        .sendMultipartForm(body)
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void uploadMimeTypePdf(TestContext context) {
    WebClient webClient = WebClient.create(vertx);

    MultipartForm body = MultipartForm.create()
        .binaryFileUpload("records", "records.mrc", Buffer.buffer(),  "application/pdf");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload/records")
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

}
