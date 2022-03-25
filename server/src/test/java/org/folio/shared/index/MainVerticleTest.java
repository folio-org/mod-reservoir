package org.folio.shared.index;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.config.HttpClientConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.tlib.postgres.testing.TenantPgPoolContainer;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {
  private final static Logger log = LogManager.getLogger("MainVerticleTest");

  static Vertx vertx;
  static final int OKAPI_PORT = 9230;
  static final String OKAPI_URL = "http://localhost:" + OKAPI_PORT;
  static final int MODULE_PORT = 9231;
  static final String MODULE_URL = "http://localhost:" + MODULE_PORT;
  static String tenant1 = "tenant1";

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  @BeforeClass
  public static void beforeClass(TestContext context) throws IOException {
    vertx = Vertx.vertx();
    WebClient webClient = WebClient.create(vertx);

    RestAssured.config=RestAssuredConfig.config()
        .httpClient(HttpClientConfig.httpClientConfig()
            .setParam("http.socket.timeout", 10000)
            .setParam("http.connection.timeout", 5000));

    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.baseURI = OKAPI_URL;
    RestAssured.requestSpecification = new RequestSpecBuilder().build();

    // deploy okapi
    DeploymentOptions okapiOptions = new DeploymentOptions();
    okapiOptions.setConfig(new JsonObject()
        .put("port", Integer.toString(OKAPI_PORT))
        .put("mode", "dev")
    );
    Future<Void> f = vertx.deployVerticle(new org.folio.okapi.MainVerticle(), okapiOptions).mapEmpty();

    // deploy this module
    f = f.compose(e -> {
      DeploymentOptions deploymentOptions = new DeploymentOptions();
      deploymentOptions.setConfig(new JsonObject().put("port", Integer.toString(MODULE_PORT)));
      return vertx.deployVerticle(new MainVerticle(), deploymentOptions).mapEmpty();
    });

    String md = Files.readString(Path.of("../descriptors/ModuleDescriptor-template.json"))
        .replace("${artifactId}", "mod-shared-index")
        .replace("${version}", "1.0.0");

    // register module
    f = f.compose(t ->
        webClient.postAbs(OKAPI_URL + "/_/proxy/modules")
            .expect(ResponsePredicate.SC_CREATED)
            .sendJsonObject(new JsonObject(md))
            .mapEmpty());

    // tell okapi where our module is running
    f = f.compose(t ->
        webClient.postAbs(OKAPI_URL + "/_/discovery/modules")
            .expect(ResponsePredicate.SC_CREATED)
            .sendJsonObject(new JsonObject()
                    .put("instId", "mod-shared-index-1.0.0")
                    .put("srvcId", "mod-shared-index-1.0.0")
                    .put("url", MODULE_URL))
            .mapEmpty());

    // create tenant
    f = f.compose(t ->
        webClient.postAbs(OKAPI_URL + "/_/proxy/tenants")
            .expect(ResponsePredicate.SC_CREATED)
            .sendJsonObject(new JsonObject().put("id", tenant1))
            .mapEmpty());

    // enable module for tenant
    f = f.compose(e ->
        webClient.postAbs(OKAPI_URL + "/_/proxy/tenants/" + tenant1 + "/install")
            .expect(ResponsePredicate.SC_OK)
            .sendJson(new JsonArray().add(new JsonObject()
                .put("id", "mod-shared-index")
                .put("action", "enable")))
                .mapEmpty());
    f.onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    tenantOp(context, tenant1, new JsonObject().put("module_from", "mod-shared-index-1.0.0"), null);
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  /**
   * Test utility for calling tenant init
   * @param context test context
   * @param tenant tenant that we're dealing with.
   * @param tenantAttributes tenant attributes as it would come from Okapi install.
   * @param expectedError error to expect (null for expecting no error)
   */
  static void tenantOp(TestContext context, String tenant, JsonObject tenantAttributes, String expectedError) {
    ExtractableResponse<Response> response = RestAssured.given()
        .baseUri(MODULE_URL)
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body(tenantAttributes.encode())
        .post("/_/tenant")
        .then().statusCode(201)
        .header("Content-Type", is("application/json"))
        .body("tenant", is(tenant))
        .extract();

    String location = response.header("Location");
    JsonObject tenantJob = new JsonObject(response.asString());
    context.assertEquals("/_/tenant/" + tenantJob.getString("id"), location);

    response = RestAssured.given()
        .baseUri(MODULE_URL)
        .header(XOkapiHeaders.TENANT, tenant)
        .get(location + "?wait=10000")
        .then().statusCode(200)
        .extract();

    context.assertTrue(response.path("complete"));
    context.assertEquals(expectedError, response.path("error"));

    RestAssured.given()
        .baseUri(MODULE_URL)
        .header(XOkapiHeaders.TENANT, tenant)
        .delete(location)
        .then().statusCode(204);
  }

  @Test
  public void testAdminHealth() {
    RestAssured.given()
        .baseUri(MODULE_URL)
        .get("/admin/health")
        .then().statusCode(200)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testGetSharedRecordsUnknownTenant() {
    String tenant = "unknowntenant";
    RestAssured.given()
        .baseUri(MODULE_URL)
        .header(XOkapiHeaders.TENANT, tenant)
        .get("/shared-index/records")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("ERROR: relation \"unknowntenant_mod_shared_index.bib_record\" does not exist (42P01)"));
  }

  @Test
  public void testGetSharedRecordsBadCqlField() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/shared-index/records?query=foo=bar")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("Unsupported CQL index: foo"));
  }

  @Test
  public void testBadTenantName() {
    String tenant = "1234"; // bad tenant name!

    String sourceId = UUID.randomUUID().toString();
    JsonArray records = new JsonArray()
        .add(new JsonObject()
            .put("localId", "HRID01")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", "1"))
        );
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    RestAssured.given()
        .baseUri(MODULE_URL)
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/shared-index/records")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("X-Okapi-Tenant header must match"));
  }

  @Test
  public void putSharedTitleUnknownTenant() {
    String sourceId = UUID.randomUUID().toString();
    JsonArray records = new JsonArray()
        .add(new JsonObject()
            .put("localId", "HRID01")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", "1"))
        );
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, "unknowntenant")
        .baseUri(MODULE_URL)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/shared-index/records")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("ERROR: relation \"unknowntenant_mod_shared_index.bib_record\" does not exist (42P01)"));
  }

  @Test
  public void matchKeysNonExistingMethod() {
    JsonObject matchKey = new JsonObject()
        .put("id", "xx")
        .put("method", "other")
        .put("params", new JsonObject());

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(Matchers.is("Non-existing method 'other'"));
  }

  @Test
  public void matchKeysOK() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/shared-index/config/matchkeys")
        .then().statusCode(200)
        .contentType("application/json")
        .body("matchKeys", is(empty()))
        .body("resultInfo.totalRecords", is(0));

    JsonObject matchKey = new JsonObject()
        .put("id", "10a")
        .put("method", "jsonpath")
        .put("params",new JsonObject().put("marc", "$.fields.010.subfields[*].a"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is("MatchKey " + matchKey.getString("id") + " not found"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(200)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/shared-index/config/matchkeys")
        .then().statusCode(200)
        .contentType("application/json")
        .body("matchKeys", hasSize(1))
        .body("matchKeys[0].id", is(matchKey.getString("id")))
        .body("matchKeys[0].method", is(matchKey.getString("method")))
        // should really check that params are same
        .body("resultInfo.totalRecords", is(1));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/shared-index/config/matchkeys?query=method=" + matchKey.getString("method"))
        .then().statusCode(200)
        .contentType("application/json")
        .body("matchKeys", hasSize(1))
        .body("matchKeys[0].id", is(matchKey.getString("id")))
        .body("matchKeys[0].method", is(matchKey.getString("method")))
        .body("resultInfo.totalRecords", is(1));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404);
  }

  @Test
  public void putSharedRecordsException() {
    String sourceId = UUID.randomUUID().toString();
    JsonArray records = new JsonArray()
        .add(new JsonObject()
            .put("localId", "HRID01")
            .put("marcPayload", new JsonArray())
            .put("inventoryPayload", new JsonObject().put("isbn", "1"))
        );
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/shared-index/records")
        .then().statusCode(400)
        .body(containsString("Validation error for body application/json: input don't match type OBJECT"));
  }

  @Test
  public void putSharedRecords() {
    String sourceId = UUID.randomUUID().toString();
    JsonArray records = new JsonArray()
        .add(new JsonObject()
            .put("localId", "HRID01")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", "1"))
        )
        .add(new JsonObject()
            .put("localId", "HRID02")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", "2"))
        );
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/shared-index/records")
        .then().statusCode(200);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/records")
        .then().statusCode(200)
        .body("resultInfo.totalRecords", greaterThanOrEqualTo(2));

    String res = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/records?query=sourceId==" + sourceId)
        .then().statusCode(200)
        .body("items", hasSize(2))
        .body("items[0].sourceId", is(sourceId))
        .body("items[1].sourceId", is(sourceId))
        .body("resultInfo.totalRecords", is(2))
        .extract().body().asString();
    JsonObject jsonResponse = new JsonObject(res);
    String globalId = jsonResponse.getJsonArray("items").getJsonObject(0).getString("globalId");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/records/" + globalId)
        .then().statusCode(200)
        .body("sourceId", is(sourceId));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/records/" + UUID.randomUUID())
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/records?query=sourceId==" + UUID.randomUUID())
        .then().statusCode(200)
        .body("items", hasSize(0))
        .body("resultInfo.totalRecords", is(0));

    for (int idx = 0; idx < records.size(); idx++) {
      JsonObject sharedRecord = records.getJsonObject(idx);
      RestAssured.given()
          .header(XOkapiHeaders.TENANT, tenant1)
          .header("Content-Type", "application/json")
          .get("/shared-index/records?query=localId==" + sharedRecord.getString("localId"))
          .then().statusCode(200)
          .body("items", hasSize(1))
          .body("items[0].localId", is(sharedRecord.getString("localId")))
          .body("items[0].marcPayload.leader", is(sharedRecord.getJsonObject("marcPayload").getString("leader")))
          .body("items[0].inventoryPayload.isbn", is(sharedRecord.getJsonObject("inventoryPayload").getString("isbn")))
          .body("items[0].sourceId", is(sourceId))
          .body("resultInfo.totalRecords", is(1));
    }
  }

  static void ingestRecords(JsonArray records, String sourceId) {
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/shared-index/records")
        .then().statusCode(200);
  }

  @Test
  public void testMatchKeys() {
    String sourceId1 = UUID.randomUUID().toString();
    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("1")))
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("2")))
        );

    ingestRecords(records1, sourceId1);
    String sourceId2 = UUID.randomUUID().toString();
    JsonArray records2 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S201")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("1")))
        )
        .add(new JsonObject()
            .put("localId", "S202")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("2")))
        )
        .add(new JsonObject()
            .put("localId", "S203")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("3")))
        );
    ingestRecords(records2, sourceId2);

    JsonObject matchKey = new JsonObject()
        .put("id", "isbn")
        .put("method", "jsonpath")
        .put("params", new JsonObject().put("inventory", "$.isbn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

    // populate first time
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/shared-index/config/matchkeys/" + matchKey.getString("id") + "/initialize")
        .then().statusCode(200)
        .contentType("application/json");

    // populate again (to check it's reset and we avoid hitting constraints)
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/shared-index/config/matchkeys/" + matchKey.getString("id") + "/initialize")
        .then().statusCode(200)
        .contentType("application/json");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/shared-index/config/matchkeys/" + matchKey.getString("id") + "/initialize")
        .then().statusCode(404)
        .contentType("text/plain")
        .body(is("MatchKey isbn not found"));
  }

  @Test
  public void upgradeDb(TestContext context) {
    String tenant = "tenant2";
    tenantOp(context, tenant, new JsonObject()
        .put("module_to", "mod-shared-index-1.0.0"), null);
    tenantOp(context, tenant, new JsonObject()
        .put("module_from", "mod-shared-index-1.0.0")
        .put("module_to", "mod-shared-index-1.0.1"), null);
  }
}
