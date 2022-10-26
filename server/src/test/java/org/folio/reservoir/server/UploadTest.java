package org.folio.reservoir.server;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.multipart.MultipartForm;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@RunWith(VertxUnitRunner.class)
public class UploadTest extends TestBase {

  @Test
  public void uploadEmpty(TestContext context) {
    WebClient webClient = WebClient.create(vertx);

    MultipartForm body = MultipartForm.create()
        .attribute("sourceId", "SOURCE-1")
        .attribute("sourceVersion", "1")
        .attribute("localIdPath", "path")
        .binaryFileUpload("records", "records.mrc", Buffer.buffer(), "application/octet-stream");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload/records")
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .expect(ResponsePredicate.SC_OK)
        .sendMultipartForm(body)
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void uploadMimeTypeApplicationMarc(TestContext context) {
    WebClient webClient = WebClient.create(vertx);

    MultipartForm body = MultipartForm.create()
        .attribute("sourceId", "SOURCE-1")
        .attribute("sourceVersion", "1")
        .attribute("localIdPath", "path")
        .binaryFileUpload("records", "records.mrc", Buffer.buffer(),  "application/marc");

    webClient.postAbs(OKAPI_URL + "/reservoir/upload/records")
        .putHeader(XOkapiHeaders.TENANT, TENANT_1)
        .expect(ResponsePredicate.SC_BAD_REQUEST)
        .sendMultipartForm(body)
        .onComplete(context.asyncAssertSuccess(res ->
          assertThat(res.bodyAsString(), containsString("File with content type \\Qapplication/octet-stream\\E and name records is missing"))
        ));
  }

}
