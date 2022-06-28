package org.folio.metastorage.server;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.config.HttpClientConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.folio.metastorage.module.ModuleCache;
import org.folio.metastorage.module.impl.ModuleScripts;
import org.folio.metastorage.server.entity.CodeModuleEntity;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.tlib.postgres.testing.TenantPgPoolContainer;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.xml.sax.SAXException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {
  private final static Logger log = LogManager.getLogger("MainVerticleTest");

  static Vertx vertx;
  static final int OKAPI_PORT = 9230;
  static final String OKAPI_URL = "http://localhost:" + OKAPI_PORT;
  static final int MODULE_PORT = 9231;
  static final String MODULE_URL = "http://localhost:" + MODULE_PORT;
  static String tenant1 = "tenant1";
  static final int CODE_MODULES_PORT = 9235;
  static final int MOCK_PORT = 9232;
  static final int UNUSED_PORT = 9233;
  static final int NET_PORT = 9234;
  static final String MOCK_URL = "http://localhost:" + MOCK_PORT;
  static final String TENANT_1 = "tenant1";
  static final String TENANT_2 = "tenant2";
  static final String PMH_CLIENT_ID = "1";
  static final String SOURCE_ID_1 = "SOURCE-1";

  static int mockStatus;

  static String mockBody;

  static String mockContentType;

  static Validator oaiSchemaValidator;

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  @BeforeClass
  public static void beforeClass(TestContext context) throws IOException, SAXException {
    URL schemaFile = MainVerticleTest.class.getResource("/OAI-PMH.xsd");
    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema schema = schemaFactory.newSchema(schemaFile);
    oaiSchemaValidator = schema.newValidator();

    vertx = Vertx.vertx();
    WebClient webClient = WebClient.create(vertx);

    RestAssured.config = RestAssuredConfig.config()
        .httpClient(HttpClientConfig.httpClientConfig()
            .setParam("http.socket.timeout", 15000)
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
            .sendJsonObject(new JsonObject().put("id", TENANT_1))
            .mapEmpty());

    // enable module for tenant
    f = f.compose(e ->
        webClient.postAbs(OKAPI_URL + "/_/proxy/tenants/" + TENANT_1 + "/install")
            .expect(ResponsePredicate.SC_OK)
            .sendJson(new JsonArray().add(new JsonObject()
                .put("id", "mod-shared-index")
                .put("action", "enable")))
            .mapEmpty());

    f = f.compose(t ->
        webClient.postAbs(OKAPI_URL + "/_/proxy/tenants")
            .expect(ResponsePredicate.SC_CREATED)
            .sendJsonObject(new JsonObject().put("id", TENANT_2))
            .mapEmpty());

    // enable module for tenant
    f = f.compose(e ->
        webClient.postAbs(OKAPI_URL + "/_/proxy/tenants/" + TENANT_2 + "/install")
            .expect(ResponsePredicate.SC_OK)
            .sendJson(new JsonArray().add(new JsonObject()
                .put("id", "mod-shared-index")
                .put("action", "enable")))
            .mapEmpty());

    Router router = Router.router(vertx);
    router.get("/mock/oai").handler(c -> {
      vertx.setTimer(10, x -> {
        c.response().setStatusCode(mockStatus);
        c.response().putHeader("Content-Type", mockContentType);
        c.response().end(mockBody);
      });
    });
    HttpServer httpServer = vertx.createHttpServer().requestHandler(router);
    f = f.compose(e -> httpServer.listen(MOCK_PORT).mapEmpty());
    f = f.compose(e -> ModuleScripts.serveModules(vertx, CODE_MODULES_PORT).mapEmpty());
    NetServer netServer = vertx.createNetServer().connectHandler(socket -> socket.close());
    f = f.compose(x -> netServer.listen(NET_PORT).mapEmpty());
    f.onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    tenantOp(context, TENANT_1, new JsonObject().put("module_from", "mod-shared-index-1.0.0"), null);
    tenantOp(context, TENANT_2, new JsonObject().put("module_from", "mod-shared-index-1.0.0"), null);
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  @After
  public void after(TestContext context) {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/meta-storage/config/oai")
        .then()
        .statusCode(204);
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/issn");
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/isbn");
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records");
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/pmh-clients/" + PMH_CLIENT_ID);
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records");
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .delete("/meta-storage/pmh-clients/" + PMH_CLIENT_ID);
  }

  /**
   * Test utility for calling tenant init
   *
   * @param context          test context
   * @param tenant           tenant that we're dealing with.
   * @param tenantAttributes tenant attributes as it would come from Okapi install.
   * @param expectedError    error to expect (null for expecting no error)
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
  public void testGetGlobalRecordsUnknownTenant() {
    String tenant = "unknowntenant";
    RestAssured.given()
        .baseUri(MODULE_URL)
        .header(XOkapiHeaders.TENANT, tenant)
        .get("/meta-storage/records")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("ERROR: relation \"unknowntenant_mod_meta_storage.global_records\" does not exist (42P01)"));
  }

  @Test
  public void testGetGlobalRecordsBadCqlField() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("query", "foo=bar")
        .get("/meta-storage/records")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("Unsupported CQL index: foo"));
  }

  @Test
  public void testBadTenantName() {
    String tenant = "1234"; // bad tenant name!

    String sourceId = "SOURCE-1";
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
        .put("/meta-storage/records")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("X-Okapi-Tenant header must match"));
  }

  @Test
  public void ingestTitleUnknownTenant() {
    String sourceId = "SOURCE-1";
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
        .put("/meta-storage/records")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString(" does not exist (42P01)"));
  }

  @Test
  public void matchKeysNonExistingMethod() {
    JsonObject matchKey = new JsonObject()
        .put("id", "xx")
        .put("method", "other")
        .put("params", new JsonObject());

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/meta-storage/config/matchkeys")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(Matchers.is("Non-existing method 'other'"));
  }

  @Test
  public void matchKeysOK() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("count", "none")
        .get("/meta-storage/config/matchkeys")
        .then().statusCode(200)
        .contentType("application/json")
        .body("matchKeys", is(empty()))
        .body("resultInfo.totalRecords", is(nullValue()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("count", "foo")
        .get("/meta-storage/config/matchkeys")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(containsString("Validation error"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("count", "exact")
        .get("/meta-storage/config/matchkeys")
        .then().statusCode(200)
        .contentType("application/json")
        .body("matchKeys", is(empty()))
        .body("resultInfo.totalRecords", is(0));

    JsonObject matchKey = new JsonObject()
        .put("id", "10a")
        .put("method", "jsonpath")
        .put("params", new JsonObject().put("marc", "$.fields.010.subfields[*].a"))
        .put("update", "ingest");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is("MatchKey " + matchKey.getString("id") + " not found"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/meta-storage/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/meta-storage/config/matchkeys")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(containsString("duplicate key value violates unique constraint"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(200)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/config/matchkeys")
        .then().statusCode(200)
        .contentType("application/json")
        .body("matchKeys", hasSize(1))
        .body("matchKeys[0].id", is(matchKey.getString("id")))
        .body("matchKeys[0].method", is(matchKey.getString("method")))
        .body("matchKeys[0].update", is(matchKey.getString("update")));
        // should really check that params are same

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/config/matchkeys?query=method=" + matchKey.getString("method"))
        .then().statusCode(200)
        .contentType("application/json")
        .body("matchKeys", hasSize(1))
        .body("matchKeys[0].id", is(matchKey.getString("id")))
        .body("matchKeys[0].method", is(matchKey.getString("method")))
        .body("matchKeys[0].update", is(matchKey.getString("update")));

    matchKey.put("update", "manual");
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(200)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .delete("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .delete("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404);
  }

  @Test
  public void ingestRecordsPayloadMissing() {
    String sourceId = "SOURCE-1";
    JsonArray records = new JsonArray()
        .add(new JsonObject()
            .put("localId", "HRID01"));
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/meta-storage/records")
        .then().statusCode(400)
        .body(containsString("payload required"));
  }

  @Test
  public void ingestRecordsLocalIdMissing() {
    String sourceId = "SOURCE-1";
    JsonArray records = new JsonArray()
        .add(new JsonObject()
            .put("payload", new JsonObject()));
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/meta-storage/records")
        .then().statusCode(400)
        .body(containsString("localId required"));
  }

  @Test
  public void ingestRecords() {
    String sourceId = "SOURCE-1";
    JsonArray records = new JsonArray()
        .add(new JsonObject()
            .put("localId", "HRID01")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", "1")))
        )
        .add(new JsonObject()
            .put("localId", "HRID02")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", "2")))
        );
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/meta-storage/records")
        .then().statusCode(200);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("count", "exact")
        .get("/meta-storage/records")
        .then().statusCode(200)
        .body("items", hasSize(2))
        .body("resultInfo.totalRecords", is(2));

    String res = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + sourceId)
        .get("/meta-storage/records")
        .then().statusCode(200)
        .body("items", hasSize(2))
        .body("resultInfo.totalRecords", is(nullValue()))
        .body("items[0].sourceId", is(sourceId))
        .body("items[1].sourceId", is(sourceId))
        .extract().body().asString();
    JsonObject jsonResponse = new JsonObject(res);
    String globalId = jsonResponse.getJsonArray("items").getJsonObject(0).getString("globalId");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/records/" + globalId)
        .then().statusCode(200)
        .body("sourceId", is(sourceId));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/records/" + UUID.randomUUID())
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + UUID.randomUUID())
        .param("count", "exact")
        .get("/meta-storage/records")
        .then().statusCode(200)
        .body("items", hasSize(0))
        .body("resultInfo.totalRecords", is(0));

    for (int idx = 0; idx < records.size(); idx++) {
      JsonObject sharedRecord = records.getJsonObject(idx);
      JsonObject payload = sharedRecord.getJsonObject("payload");
      RestAssured.given()
          .header(XOkapiHeaders.TENANT, TENANT_1)
          .header("Content-Type", "application/json")
          .param("count", "exact")
          .param("query", "localId==" + sharedRecord.getString("localId"))
          .get("/meta-storage/records")
          .then().statusCode(200)
          .body("items", hasSize(1))
          .body("items[0].localId", is(sharedRecord.getString("localId")))
          .body("items[0].payload.marc.leader", is(payload.getJsonObject("marc").getString("leader")))
          .body("items[0].payload.inventory.isbn", is(payload.getJsonObject("inventory").getString("isbn")))
          .body("items[0].sourceId", is(sourceId))
          .body("resultInfo.totalRecords", is(1));
    }

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);


  }

  static String verifyOaiResponse(String s, String verb, List<String> identifiers, int length, JsonArray expRecords)
      throws XMLStreamException, IOException, SAXException {
    InputStream stream = new ByteArrayInputStream(s.getBytes());
    Source source = new StreamSource(stream);
    oaiSchemaValidator.validate(source);

    stream = new ByteArrayInputStream(s.getBytes());
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    String path = "";
    boolean foundEnvelope = false;
    String resumptionToken = null;

    int offset = 0;
    int level = 0;
    int numRecsOrDels = 0;
    JsonArray records = new JsonArray();
    JsonObject record = null;
    JsonArray fields = null;
    JsonArray subfields = null;
    while (xmlStreamReader.hasNext()) {
      int event = xmlStreamReader.next();
      if (event == XMLStreamConstants.START_ELEMENT) {
        level++;
        String elem = xmlStreamReader.getLocalName();
        path = path + "/" + elem;
        if (level == 2 && elem == verb) {
          foundEnvelope = true;
        }
        if ("identifier".equals(elem) && xmlStreamReader.hasNext()) {
          event = xmlStreamReader.next();
          if (event == XMLStreamConstants.CHARACTERS) {
            identifiers.add(xmlStreamReader.getText());
          }
        }
        if (level == 3 && ("record".equals(elem) || "header".equals(elem))) {
          //ListRecords has 'record' while ListIdentifiers has 'header'
          offset++;
        }
        if (level == 4 && "header".equals(elem)) {
          String status = xmlStreamReader.getAttributeValue(null, "status");
          if ("deleted".equals(status)) {
            numRecsOrDels++;
          }
        }
        if (level == 3 && "resumptionToken".equals(elem)) {
          event = xmlStreamReader.next();
          if (event == XMLStreamConstants.CHARACTERS) {
            resumptionToken = xmlStreamReader.getText();
          }
        }
        if (level == 4 && "metadata".equals(elem)) {
          //nothing
        }
        if (level == 5 && "record".equals(elem)) {
          numRecsOrDels++;
          record = new JsonObject();
          fields = new JsonArray();
          record.put("fields", fields);
          records.add(record);
        }
        if (level == 6 && "leader".equals(elem)) {
          event = xmlStreamReader.next();
          String leader = null;
          if (event == XMLStreamConstants.CHARACTERS) {
            leader = xmlStreamReader.getText();
          }
          record.put("leader", leader);
        }
        if (level == 6 && "datafield".equals(elem)) {
          String tag = xmlStreamReader.getAttributeValue(null, "tag");
          String ind1 = xmlStreamReader.getAttributeValue(null, "ind1");
          String ind2 = xmlStreamReader.getAttributeValue(null, "ind2");
          subfields = new JsonArray();
          fields.add(new JsonObject()
            .put(tag, new JsonObject()
              .put("ind1", ind1)
              .put("ind2", ind2)
              .put("subfields", subfields)));
        }
        if (level == 7 && "subfield".equals(elem)) {
          String code = xmlStreamReader.getAttributeValue(null, "code");
          String value = null;
          event = xmlStreamReader.next();
          if (event == XMLStreamConstants.CHARACTERS) {
            value = xmlStreamReader.getText();
          }
          subfields.add(new JsonObject().put(code, value));
        }
      } else if (event == XMLStreamConstants.END_ELEMENT) {
        int off = path.lastIndexOf('/');
        path = path.substring(0, off);
        level--;
      }
    }
    if (length != -1) {
      Assert.assertEquals(s, length, offset);
    }
    if (length > 0) {
      Assert.assertTrue(s, foundEnvelope);
    }
    if (length != -1 && verb.equals("ListRecords")) {
      Assert.assertEquals(s, length, numRecsOrDels);
      if (expRecords != null) {
        verifyMarc(expRecords , records);
      }
    }
    return resumptionToken;
  }

  static void verifyMarc(JsonArray expMarcs, JsonArray actMarcs) {
    Assert.assertEquals("size", expMarcs.size(), actMarcs.size());
    for (int i=0; i < expMarcs.size(); i++) {
      JsonObject expMarc = expMarcs.getJsonObject(i);
      JsonObject actMarc = actMarcs.getJsonObject(i);
      Assert.assertEquals("leader",
        expMarc.getString("leader"),
        actMarc.getString("leader"));
      JsonArray expFields = expMarc.getJsonArray("fields");
      JsonArray actFields = actMarc.getJsonArray("fields");
      Assert.assertNotNull("["+i+"] expected 'fields' not null", expFields);
      Assert.assertNotNull("["+i+"] actual 'fields' not null", actFields);
      Assert.assertEquals("["+i+"] 'fields' size", expFields.size(), actFields.size());
      for (int j=0; j < expFields.size(); j++) {
        JsonObject expField = expFields.getJsonObject(j);
        JsonObject actField = actFields.getJsonObject(j);
        for (String expFieldName : expField.getMap().keySet()) {
          JsonObject expFieldValue = expField.getJsonObject(expFieldName);
          JsonObject actFieldValue = actField.getJsonObject(expFieldName);
          Assert.assertEquals("["+i+"] "+expFieldName + "/ind1",
            expFieldValue.getString("ind1"),
            actFieldValue.getString("ind1")
          );
          Assert.assertEquals("["+i+"] "+expFieldName + "/ind2",
            expFieldValue.getString("ind2"),
            actFieldValue.getString("ind2")
          );
          JsonArray expSubFields = expFieldValue.getJsonArray("subfields");
          JsonArray actSubFields = actFieldValue.getJsonArray("subfields");
          Assert.assertNotNull("["+i+"] "+expFieldName+" expected subfields not null", expSubFields);
          Assert.assertNotNull("["+i+"] "+expFieldName+" actual subfields not null", actSubFields);
          Assert.assertEquals("["+i+"] "+expFieldName+"/'subfields' size", expSubFields.size(), actSubFields.size());
          for (int k=0; k < expSubFields.size(); k++) {
            JsonObject expSubField = expSubFields.getJsonObject(k);
            JsonObject actSubField = actSubFields.getJsonObject(k);
            for (String expSubFieldName : expSubField.getMap().keySet()) {
              String expSubFieldValue = expSubField.getString(expSubFieldName);
              String actSubFieldValue = actSubField.getString(expSubFieldName);
              if (!"DO_NOT_ASSERT".equals(expSubFieldValue)) {
                Assert.assertEquals("["+i+"] "+expFieldName + "/" + expSubFieldName,
                  expSubFieldValue,
                  actSubFieldValue);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Check that each records in each cluster contains exactly set of localIds.
   *
   * @param s        cluster response
   * @param localIds expected localId values for each cluster
   */
  static void verifyClusterResponse(String s, List<String>... localIds) {
    List<Set<String>> foundIds = new ArrayList<>();
    JsonObject clusterResponse = new JsonObject(s);
    JsonArray items = clusterResponse.getJsonArray("items");
    for (int i = 0; i < items.size(); i++) {
      JsonArray records = items.getJsonObject(i).getJsonArray("records");
      Set<String> ids = new HashSet<>();
      for (int j = 0; j < records.size(); j++) {
        String localId = records.getJsonObject(j).getString("localId");
        Assert.assertTrue(ids.add(localId)); // repeated localId values would be wrong.
      }
      foundIds.add(ids);
    }
    for (List<String> idList : localIds) {
      String candidate = idList.get(0); // just use first on to find the cluster
      for (Set<String> foundId : foundIds) {
        if (foundId.contains(candidate)) {
          for (String id : idList) {
            Assert.assertTrue(s, foundId.remove(id));
          }
          Assert.assertTrue(s, foundId.isEmpty());
          break;
        }
      }
    }
    for (Set<String> foundId : foundIds) {
      Assert.assertTrue(s, foundId.isEmpty());
    }
  }

  static void ingestRecords(JsonArray records, String sourceId) {
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/meta-storage/records")
        .then().statusCode(200);
  }

  @Test
  public void testMatchKeysIngest() {
    JsonObject matchKey = createIsbnMatchKey();

    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("1")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("2").add("3")))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + SOURCE_ID_1)
        .get("/meta-storage/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + SOURCE_ID_1)
        .get("/meta-storage/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2));

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .body("items[0].records", hasSize(1))
        .body("items[1].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "matchValue=3")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .body("items[0].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S102"));

    ingestRecords(records1, SOURCE_ID_1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + SOURCE_ID_1)
        .get("/meta-storage/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .body("items[0].records", hasSize(1))
        .body("items[1].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);
  }

  @Test
  public void testMatchKeyIdMissing() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/clusters")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(containsString("Missing parameter matchkeyid"));
  }

  @Test
  public void testMatchKeyIdNotFound() {
    String id = UUID.randomUUID().toString();
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("matchkeyid", id)
        .header("Content-Type", "application/json")
        .get("/meta-storage/clusters")
        .then().statusCode(404)
        .contentType("text/plain")
        .body(is("MatchKey " + id + " not found"));
  }

  JsonObject createIssnMatchKey() {
    JsonObject matchKey = new JsonObject()
        .put("id", "issn")
        .put("method", "jsonpath")
        // update = ingest is the default
        .put("params", new JsonObject().put("expr", "$.inventory.issn[*]"));
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/meta-storage/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));
    return matchKey;
  }

  JsonObject createIsbnMatchKey() {
    return createIsbnMatchKey(null);
  }

  JsonObject createIsbnMatchKey(String updateValue) {
    JsonObject matchKey = new JsonObject()
        .put("id", "isbn")
        .put("method", "jsonpath")
        .put("params", new JsonObject().put("expr", "$.inventory.isbn[*]"));

    if (updateValue != null) {
      matchKey.put("update", updateValue);
    }
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/meta-storage/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));
    return matchKey;
  }

  @Test
  public void testClustersSameKey() {
    createIssnMatchKey();

    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("issn", new JsonArray().add("1")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("issn", new JsonArray().add("1")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S103")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("issn", new JsonArray().add("1")))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "issn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101", "S102", "S103"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/issn")
        .then().statusCode(204);
  }

  @Test
  public void testClustersLargeKey() {
    createIssnMatchKey();

    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject()
                    .put("issn", new JsonArray().add("1".repeat(3600))))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "issn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/issn")
        .then().statusCode(204);
  }

  @Test
  public void testClustersMove() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/clusters")
        .then().statusCode(400);

    createIsbnMatchKey();
    createIssnMatchKey();

    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("1"))
                    .put("issn", new JsonArray().add("01"))
                )
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("2").add("3"))
                    .put("issn", new JsonArray().add("01"))
                )
            )
        );
    log.info("phase 1: insert two separate isbn recs, but one with issn");
    ingestRecords(records1, SOURCE_ID_1);

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "issn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .body("items[0].records", hasSize(2))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101", "S102"));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .body("items[0].records", hasSize(1))
        .body("items[1].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    String clusterId = new JsonObject(s).getJsonArray("items").getJsonObject(0).getString("clusterId");
    String datestamp = new JsonObject(s).getJsonArray("items").getJsonObject(0).getString("datestamp");
    Assert.assertNotNull(datestamp);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "clusterId=" + clusterId)
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .body("items[0].records", hasSize(1))
        .body("items[0].clusterId", is(clusterId))
        .body("items[0].datestamp", is(datestamp));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/clusters/" + clusterId)
        .then().statusCode(200)
        .contentType("application/json")
        .body("records", hasSize(1))
        .body("clusterId", is(clusterId));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/clusters/" + UUID.randomUUID())
        .then().statusCode(404);

    log.info("phase 2: S101 from 1 to 4");
    records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("4")))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "issn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .body("items[0].records", hasSize(1))
        .body("items[1].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    log.info("phase 3: S101 from 4 to 3");
    records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("3")))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101", "S102"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/isbn")
        .then().statusCode(204);
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/issn")
        .then().statusCode(204);
  }

  @Test
  public void testDeleteGlobalRecords() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + UUID.randomUUID())
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .delete("/meta-storage/records")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(is("Must specify query for delete records"));

    JsonObject matchKey = createIsbnMatchKey();

    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0101   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("1")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0102   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("2")))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("count", "exact")
        .get("/meta-storage/records")
        .then().statusCode(200)
        .body("resultInfo.totalRecords", is(2));

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "localId=S102")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("count", "exact")
        .get("/meta-storage/records")
        .then().statusCode(200)
        .body("resultInfo.totalRecords", is(1));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "localId=S101")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("count", "exact")
        .get("/meta-storage/records")
        .then().statusCode(200)
        .body("resultInfo.totalRecords", is(0));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(0));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);
  }

  @Test
  public void testEmptyMatchKeys() {
    JsonObject matchKey = createIsbnMatchKey();

    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0101   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("1")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0102   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray()))
            )
        )
        .add(new JsonObject()
            .put("localId", "S103")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0102   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray()))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("count", "exact")
        .get("/meta-storage/records")
        .then().statusCode(200)
        .body("resultInfo.totalRecords", is(3));

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(3))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"), List.of("S103"));

    records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S103")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0102   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("1")))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);
    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101", "S103"), List.of("S102"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);
  }

  @Test
  public void testMatchKeysManual() {
    JsonObject matchKey = createIsbnMatchKey("manual");

    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0101   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("1")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0102   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("2")))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    // populate first time
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/meta-storage/config/matchkeys/" + matchKey.getString("id") + "/initialize")
        .then().statusCode(200)
        .contentType("application/json")
        .body("totalRecords", is(2))
    ;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + SOURCE_ID_1)
        .get("/meta-storage/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2));

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .body("items[0].records", hasSize(1))
        .body("items[1].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    String sourceId2 = "SOURCE-2";
    JsonArray records2 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S201")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0201   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("3")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S202")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0202   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("1").add("2").add("3").add("4")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S203")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0203   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("5").add("6")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S204")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0204   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("5")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S205")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  0204   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("4")))
            )
        );
    ingestRecords(records2, sourceId2);

    // populate again with both sources
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/meta-storage/config/matchkeys/" + matchKey.getString("id") + "/initialize")
        .then().statusCode(200)
        .contentType("application/json")
        .body("totalRecords", is(7))
    ;

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/meta-storage/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .extract().body().asString();

    verifyClusterResponse(s, List.of("S101", "S102", "S201", "S202", "S205"), List.of("S203", "S204"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "localId=S101 and sourceId=" + SOURCE_ID_1)
        .get("/meta-storage/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "localId==notfound")
        .get("/meta-storage/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(0));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/meta-storage/config/matchkeys/" + matchKey.getString("id") + "/initialize")
        .then().statusCode(404)
        .contentType("text/plain")
        .body(is("MatchKey isbn not found"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);
  }

  @Test
  public void testOaiDiagnostics(TestContext context) {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"badVerb\">missing verb</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("set", "isbn")
        .param("verb", "noop")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"badVerb\">noop</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "GetRecord")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("error code=\"badArgument\">missing identifier</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "badmetadataprefix")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"cannotDisseminateFormat\">only metadataPrefix &quot;marcxml&quot; supported</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"badArgument\">set &quot;null&quot; not found</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"badArgument\">set &quot;isbn&quot; not found</error>"));
  }

  @Test
  public void testIdentify() throws XMLStreamException, IOException, SAXException {
    vertx.getOrCreateContext().config().put("adminEmail", "admin@indexdata.com");
    List<String> identifiers = new LinkedList<>();
    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "Identify")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "Identify", identifiers, 0, null);
  }

  @Test
  public void testCodeModulesCRUD() {
    //GET empty list no count
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/meta-storage/config/modules")
        .then().statusCode(200)
        .contentType("application/json")
        .body("modules", is(empty()))
        .body("resultInfo.totalRecords", is(nullValue()));

    //GET empty list with count
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("count", "exact")
        .get("/meta-storage/config/modules")
        .then().statusCode(200)
        .contentType("application/json")
        .body("modules", is(empty()))
        .body("resultInfo.totalRecords", is(0));

    //POST item with bad url and nothing should be created
    CodeModuleEntity badModule = new CodeModuleEntity("empty",  "url", "transform");
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(badModule.asJson().encode())
        .post("/meta-storage/config/modules")
        .then().statusCode(400);

    CodeModuleEntity module = new CodeModuleEntity("empty",  "http://localhost:" + CODE_MODULES_PORT + "/lib/empty.mjs", "transform");

    //GET not found item
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/config/modules/" + module.getId())
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is("Module " + module.getId() + " not found"));

    //reload - not found item
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .put("/meta-storage/config/modules/" + module.getId() + "/reload")
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is("Module " + module.getId() + " not found"));

    //POST item
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(module.asJson().encode())
        .post("/meta-storage/config/modules")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(module.asJson().encode()));

    //POST same item again
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(module.asJson().encode())
        .post("/meta-storage/config/modules")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(containsString("duplicate key value violates unique constraint"));

    //GET posted item
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/config/modules/" + module.getId())
        .then().statusCode(200)
        .contentType("application/json")
        .body(Matchers.is(module.asJson().encode()));

    // reload existing module
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .put("/meta-storage/config/modules/" + module.getId() + "/reload")
        .then().statusCode(204);

    //GET item and validate it
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/meta-storage/config/modules")
        .then().statusCode(200)
        .contentType("application/json")
        .body("modules", hasSize(1))
        .body("modules[0].id", is(module.getId()))
        .body("modules[0].url", is(module.getUrl()))
        .body("modules[0].function", is(module.getFunction()));

    //GET search item and validate
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/meta-storage/config/modules?query=function=" + module.getFunction())
        .then().statusCode(200)
        .contentType("application/json")
        .body("modules", hasSize(1))
        .body("modules[0].id", is(module.getId()))
        .body("modules[0].url", is(module.getUrl()))
        .body("modules[0].function", is(module.getFunction()));

    //DELETE item
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .delete("/meta-storage/config/modules/" + module.getId())
        .then().statusCode(204);

    //DELETE item again
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .delete("/meta-storage/config/modules/" + module.getId())
        .then().statusCode(404);

    //GET deleted item
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/meta-storage/config/modules/" + module.getId())
        .then().statusCode(404);

    //PUT item to not existing
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(module.asJson().encode())
        .put("/meta-storage/config/modules/" + module.getId())
        .then()
        .statusCode(404);

    //POST item again
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(module.asJson().encode())
        .post("/meta-storage/config/modules")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(module.asJson().encode()));

    //PUT item to existing
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(module.asJson().encode())
        .put("/meta-storage/config/modules/" + module.getId())
        .then()
        .statusCode(204);

    //DELETE item
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .delete("/meta-storage/config/modules/" + module.getId())
        .then().statusCode(204);

  }

  @Test
  public void testOaiConfigRU() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/meta-storage/config/oai")
        .then()
        .statusCode(404);

    JsonObject oaiConfig = new JsonObject()
        .put("baseURL", "localhost")
        .put("adminEmail", "admin@localhost")
        .put("transformer", "transform-marc")
        .put("repositoryName", "MetaStorage OAI server");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(oaiConfig.encode())
        .put("/meta-storage/config/oai")
        .then()
        .statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/meta-storage/config/oai")
        .then()
        .statusCode(200)
        .contentType("application/json")
        .body(Matchers.is(oaiConfig.encode()));

    oaiConfig.put("badProperty", "not allower");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(oaiConfig.encode())
        .put("/meta-storage/config/oai")
        .then()
        .statusCode(400);

  }

  @Test
  @java.lang.SuppressWarnings("squid:S5961")
  public void testOaiSimple() throws XMLStreamException, IOException, SAXException {
    createIsbnMatchKey();

    List<String> identifiers = new LinkedList<>();
    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 0, null);

    createIssnMatchKey();

    JsonArray ingest1a = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject()
                    .put("leader", "00914naa  2200337   450 ")
                    .put("fields", new JsonArray()
                        .add(new JsonObject()
                            .put("999", new JsonObject()
                                .put("ind1", " ")
                                .put("ind2", " ")
                                .put("subfields", new JsonArray()
                                    .add(new JsonObject()
                                        .put("a", "S101a")
                                    )
                                    .add(new JsonObject()
                                        .put("b", "S101b")
                                    )
                                )
                            )
                        )
                    )
                )
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("1"))
                    .put("issn", new JsonArray().add("01"))
                )
            )
        );
        JsonArray ingest1b = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S102")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject()
                    .put("leader", "00914naa  2200337   450 ")
                    .put("fields", new JsonArray()
                        .add(new JsonObject()
                            .put("999", new JsonObject()
                                .put("ind1", " ")
                                .put("ind2", " ")
                                .put("subfields", new JsonArray()
                                    .add(new JsonObject()
                                        .put("a", "S102a")
                                    )
                                    .add(new JsonObject()
                                        .put("b", "S102b")
                                    )
                                )
                            )
                        )
                    )
                )
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("2").add("3"))
                    .put("issn", new JsonArray().add("01"))
                )
            )
        );
    //post records individually, otherwise the order of clusters and records in clusters is non-deterministic
    ingestRecords(ingest1a, SOURCE_ID_1);
    ingestRecords(ingest1b, SOURCE_ID_1);

    JsonArray expectedIssn = new JsonArray()
      .add(new JsonObject()
          .put("leader", "00914naa  2200337   450 ")
          .put("fields", new JsonArray()
                .add(new JsonObject()
                  .put("999", new JsonObject()
                    .put("ind1", "1")
                    .put("ind2", "0")
                    .put("subfields", new JsonArray()
                      .add(new JsonObject().put("i", "DO_NOT_ASSERT"))
                      .add(new JsonObject().put("m", "01"))
                      .add(new JsonObject().put("l", "S101"))
                      .add(new JsonObject().put("s", "SOURCE-1"))
                      .add(new JsonObject().put("l", "S102"))
                      .add(new JsonObject().put("s", "SOURCE-1"))
                    )
                  )
                )
                .add(new JsonObject()
                  .put("999", new JsonObject()
                      .put("ind1", " ")
                      .put("ind2", " ")
                      .put("subfields", new JsonArray()
                          .add(new JsonObject()
                            .put("a", "S101a")
                          )
                          .add(new JsonObject()
                            .put("b", "S101b")
                          )
                          .add(new JsonObject()
                            .put("a", "S102a")
                          )
                          .add(new JsonObject()
                            .put("b", "S102b")
                          )
                      )
                  )
                )
          )
      );

    JsonArray expectedIsbn = new JsonArray()
      .add(new JsonObject()
        .put("leader", "00914naa  2200337   450 ")
        .put("fields", new JsonArray()
            .add(new JsonObject()
              .put("999", new JsonObject()
                .put("ind1", "1")
                .put("ind2", "0")
                .put("subfields", new JsonArray()
                  .add(new JsonObject().put("i", "DO_NOT_ASSERT"))
                  .add(new JsonObject().put("m", "1"))
                  .add(new JsonObject().put("l", "S101"))
                  .add(new JsonObject().put("s", "SOURCE-1"))
                )
              )
            )
            .add(new JsonObject()
                .put("999", new JsonObject()
                    .put("ind1", " ")
                    .put("ind2", " ")
                    .put("subfields", new JsonArray()
                        .add(new JsonObject()
                            .put("a", "S101a")
                        )
                        .add(new JsonObject()
                            .put("b", "S101b")
                      )
                    )
                )
            )
        )
      )
      .add(new JsonObject()
        .put("leader", "00914naa  2200337   450 ")
        .put("fields", new JsonArray()
          .add(new JsonObject()
              .put("999", new JsonObject()
              .put("ind1", "1")
              .put("ind2", "0")
              .put("subfields", new JsonArray()
                  .add(new JsonObject().put("i", "DO_NOT_ASSERT"))
                  .add(new JsonObject().put("m", "2"))
                  .add(new JsonObject().put("m", "3"))
                  .add(new JsonObject().put("l", "S102"))
                  .add(new JsonObject().put("s", "SOURCE-1"))
              )
              )
          )
          .add(new JsonObject()
              .put("999", new JsonObject()
                  .put("ind1", " ")
                  .put("ind2", " ")
                  .put("subfields", new JsonArray()
                      .add(new JsonObject()
                          .put("a", "S102a")
                      )
                      .add(new JsonObject()
                          .put("b", "S102b")
                      )
                  )
              )
          )
        )
      );

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("set", "issn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 1, expectedIssn);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("set", "issn")
        .param("verb", "ListIdentifiers")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListIdentifiers", identifiers, 1, expectedIssn);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "GetRecord")
        .param("metadataPrefix", "marcxml")
        .param("identifier", identifiers.get(0))
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "GetRecord", identifiers, 1, expectedIssn);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "GetRecord")
        .param("metadataPrefix", "marcxml")
        .param("identifier", UUID.randomUUID().toString())
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("idDoesNotExist"));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 2, expectedIsbn);

    //configure transformers
    for (String m : List.of("marc-transformer", "empty", "throw")) {
      CodeModuleEntity module = new CodeModuleEntity(
          m, "http://localhost:" + CODE_MODULES_PORT + "/lib/" + m + ".mjs", "transform");

      //POST module configuration
      RestAssured.given()
          .header(XOkapiHeaders.TENANT, tenant1)
          .header("Content-Type", "application/json")
          .body(module.asJson().encode())
          .post("/meta-storage/config/modules")
          .then().statusCode(201)
          .contentType("application/json")
          .body(Matchers.is(module.asJson().encode()));
    }

    //PUT oai configuration
    JsonObject oaiConfig = new JsonObject()
        .put("transformer", "marc-transformer");;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(oaiConfig.encode())
        .put("/meta-storage/config/oai")
        .then()
        .statusCode(204);

    JsonArray expectedIssn2 = new JsonArray()
        .add(new JsonObject()
            .put("leader", "new leader")
            .put("fields", new JsonArray()
                  .add(new JsonObject()
                    .put("999", new JsonObject()
                        .put("ind1", " ")
                        .put("ind2", " ")
                        .put("subfields", new JsonArray()
                            .add(new JsonObject()
                              .put("a", "S101a")
                            )
                            .add(new JsonObject()
                              .put("b", "S101b")
                            )
                        )
                    )
                  )
                  .add(new JsonObject()
                    .put("999", new JsonObject()
                        .put("ind1", "1")
                        .put("ind2", "0")
                        .put("subfields", new JsonArray()
                        .add(new JsonObject().put("i", "DO_NOT_ASSERT"))
                        .add(new JsonObject().put("l", "S101"))
                        .add(new JsonObject().put("s", "SOURCE-1"))
                        )
                    )
                  )
                  .add(new JsonObject()
                    .put("999", new JsonObject()
                        .put("ind1", " ")
                        .put("ind2", " ")
                        .put("subfields", new JsonArray()
                            .add(new JsonObject()
                              .put("a", "S102a")
                            )
                            .add(new JsonObject()
                              .put("b", "S102b")
                            )
                        )
                    )
                  )
                  .add(new JsonObject()
                    .put("999", new JsonObject()
                      .put("ind1", "1")
                      .put("ind2", "0")
                      .put("subfields", new JsonArray()
                        .add(new JsonObject().put("i", "DO_NOT_ASSERT"))
                        .add(new JsonObject().put("l", "S102"))
                        .add(new JsonObject().put("s", "SOURCE-1"))
                      )
                    )
                  )
            )
        );


    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "issn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 1, expectedIssn2);

    oaiConfig = new JsonObject()
        .put("transformer", "empty");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(oaiConfig.encode())
        .put("/meta-storage/config/oai")
        .then()
        .statusCode(204);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "issn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();

    verifyOaiResponse(s, "ListRecords", identifiers, 1, null);

    oaiConfig = new JsonObject()
        .put("transformer", "throw");;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(oaiConfig.encode())
        .put("/meta-storage/config/oai")
        .then()
        .statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "issn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<!-- Failed to produce record: Error -->"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "GetRecord")
        .param("metadataPrefix", "marcxml")
        .param("identifier", identifiers.get(0))
        .get("/meta-storage/oai")
        .then().statusCode(500)
        .contentType("text/plain")
        .body(containsString("Error"));

    // OAI config with unknown transformer
    JsonObject oaiConfigBadTransformer = new JsonObject()
        .put("transformer", "doesnotexist");
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(oaiConfigBadTransformer.encode())
        .put("/meta-storage/config/oai")
        .then()
        .statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "issn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(500)
        .contentType("text/plain")
        .body(is("Transformer not found: doesnotexist"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "GetRecord")
        .param("metadataPrefix", "marcxml")
        .param("identifier", identifiers.get(0))
        .get("/meta-storage/oai")
        .then().statusCode(500)
        .contentType("text/plain")
        .body(is("Transformer not found: doesnotexist"));

    //PUT disable the transformer
    JsonObject oaiConfigOff = new JsonObject();

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(oaiConfigOff.encode())
        .put("/meta-storage/config/oai")
        .then()
        .statusCode(204);

    JsonArray ingest2 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S103")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("1").add("2"))
                    .put("issn", new JsonArray().add("02"))
                    .put("holdingsRecords", new JsonArray().add(new JsonObject()
                            .put("permanentLocationDeref", "S103")
                        )
                    )
                )
            )
        );
    ingestRecords(ingest2, SOURCE_ID_1);


    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then()
        .statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 2, null);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("set", "isbn")
        .param("verb", "ListIdentifiers")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListIdentifiers", identifiers, 2, null);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/isbn")
        .then().statusCode(204);
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/issn")
        .then().statusCode(204);
  }

  @Test
  public void testOaiDatestamp() throws XMLStreamException, InterruptedException, IOException, SAXException {
    String time0 = Instant.now(Clock.systemUTC()).minusSeconds(1L).truncatedTo(ChronoUnit.SECONDS).toString();
    String time1 = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS).toString();

    createIsbnMatchKey();

    List<String> identifiers = new LinkedList<>();
    String s;

    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("1")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("2")))
            )
        )
        .add(new JsonObject()
            .put("localId", "S103")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("3")))
            )

        );


    JsonArray expectedOAI = new JsonArray()
      .add(new JsonObject().put("leader", "00914naa  2200337   450 "))
      .add(new JsonObject().put("leader", "00914naa  2200337   450 "))
      .add(new JsonObject().put("leader", "00914naa  2200337   450 "));

    ingestRecords(records1, SOURCE_ID_1);
    String time2 = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS).toString();
    TimeUnit.SECONDS.sleep(1);
    String time3 = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS).toString();

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "ListRecords")
        .param("from", time1)
        .param("until", time2)
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 3, null);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "ListRecords")
        .param("until", time0)
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 0, null);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "ListRecords")
        .param("from", time3)
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 0, null);

    ingestRecords(records1, SOURCE_ID_1);
    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "ListRecords")
        .param("from", time3)
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 3, null);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "ListRecords")
        .param("from", "xxxx")
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("error code=\"badArgument\">bad from"));

    TimeUnit.SECONDS.sleep(1);
    String time4 = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS).toString();

    JsonArray records2 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S103")
            .put("delete", true)
        );

    ingestRecords(records2, SOURCE_ID_1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "ListRecords")
        .param("from", time4)
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 1, null);

    TimeUnit.SECONDS.sleep(1);
    String time5 = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS).toString();

    records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S104")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject().put("isbn", new JsonArray().add("1").add("2")))
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "ListRecords")
        .param("from", time5)
        .param("metadataPrefix", "marcxml")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 2, null);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/isbn")
        .then().statusCode(204);
  }

  @Test
  public void testOaiResumptionToken() throws XMLStreamException, IOException, SAXException {
    createIsbnMatchKey();

    for (int i = 0; i < 10; i++) {
      JsonArray records1 = new JsonArray()
          .add(new JsonObject()
              .put("localId", "S" + i)
              .put("payload", new JsonObject()
                  .put("marc", new JsonObject().put("leader", "00914naa  0101   450 "))
                  .put("inventory", new JsonObject().put("isbn", new JsonArray().add(Integer.toString(i))))
              )
          );
      ingestRecords(records1, SOURCE_ID_1);
    }
    List<String> identifiers = new LinkedList<>();

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .param("verb", "ListRecords")
        .param("limit", "2")
        .get("/meta-storage/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    int iter;
    for (iter = 0; iter < 10; iter++) {
      String token = verifyOaiResponse(s, "ListRecords", identifiers, -1, null);
      if (token == null) {
        break;
      }
      ResumptionToken tokenClass = new ResumptionToken(token);
      Assert.assertEquals("isbn", tokenClass.getSet());
      s = RestAssured.given()
          .header(XOkapiHeaders.TENANT, TENANT_1)
          .param("verb", "ListRecords")
          .param("limit", "2")
          .param("resumptionToken", token)
          .get("/meta-storage/oai")
          .then().statusCode(200)
          .contentType("text/xml")
          .extract().body().asString();
    }
    Assert.assertEquals(4, iter);
    Assert.assertEquals(10, identifiers.size());

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/isbn")
        .then().statusCode(204);

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

  @Test
  public void testMatchKeyStats() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/config/matchkeys/isbn/stats")
        .then().statusCode(404);

    createIsbnMatchKey();

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/config/matchkeys/isbn/stats")
        .then().statusCode(200)
        .contentType("application/json")
        .body("recordsTotal", is(0))
        .body("clustersTotal", is(0))
    ;

    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("1"))
                )
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("2").add("3"))
                )
            )
        )
        .add(new JsonObject()
            .put("localId", "S103")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("2").add("4"))
                )
            )
        )
        .add(new JsonObject()
            .put("localId", "S104")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("5")
                    )
                )
            )
        )
        .add(new JsonObject()
            .put("localId", "S105")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject())
            )
        )
        .add(new JsonObject()
            .put("localId", "S106")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject())
            )
        );
    ingestRecords(records1, SOURCE_ID_1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/config/matchkeys/isbn/stats")
        .then().statusCode(200)
        .contentType("application/json")
        .body("recordsTotal", is(6))
        .body("clustersTotal", is(5))
        .body("matchValuesPerCluster.0", is(2))
        .body("matchValuesPerCluster.1", is(2))
        .body("matchValuesPerCluster.3", is(1))
        .body("recordsPerCluster.1", is(4))
        .body("recordsPerCluster.2", is(1))
    ;

    String sourceId2 = "SOURCE-2";
    JsonArray records2 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("payload", new JsonObject()
                .put("marc", new JsonObject().put("leader", "00914naa  2200337   450 "))
                .put("inventory", new JsonObject()
                    .put("isbn", new JsonArray().add("6"))
                )
            )
        );
    ingestRecords(records2, sourceId2);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/config/matchkeys/isbn/stats")
        .then().statusCode(200)
        .contentType("application/json")
        .body("recordsTotal", is(7))
        .body("clustersTotal", is(6))
        .body("matchValuesPerCluster.0", is(2))
        .body("matchValuesPerCluster.1", is(3))
        .body("matchValuesPerCluster.3", is(1))
        .body("recordsPerCluster.1", is(5))
        .body("recordsPerCluster.2", is(1))
    ;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId = " + sourceId2)
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/config/matchkeys/isbn/stats")
        .then().statusCode(200)
        .contentType("application/json")
        .body("recordsTotal", is(6))
        .body("clustersTotal", is(5))
        .body("matchValuesPerCluster.0", is(2))
        .body("matchValuesPerCluster.1", is(2))
        .body("matchValuesPerCluster.3", is(1))
        .body("recordsPerCluster.1", is(4))
        .body("recordsPerCluster.2", is(1))
    ;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/meta-storage/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/config/matchkeys/isbn")
        .then().statusCode(204);
  }

  @Test
  public void oaiPmhClientCRUD() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID)
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is(PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients")
        .then().statusCode(200)
        .contentType("application/json")
        .body("resultInfo.totalRecords", is(0));

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", "http://localhost:" + OKAPI_PORT + " /meta-storage/oai")
        .put("sourceId", "source-1")
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .put("/meta-storage/pmh-clients/" + PMH_CLIENT_ID)
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is(PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(400);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID)
        .then().statusCode(200)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].id", is(PMH_CLIENT_ID))
        .body("items[0].url", is(oaiPmhClient.getString("url")))
        .body("resultInfo.totalRecords", is(1));

    oaiPmhClient.put("url", "http://foo.bar");
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .put("/meta-storage/pmh-clients/" + PMH_CLIENT_ID)
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].id", is(PMH_CLIENT_ID))
        .body("items[0].url", is(oaiPmhClient.getString("url")))
        .body("resultInfo.totalRecords", is(1));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/pmh-clients/" + PMH_CLIENT_ID)
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/pmh-clients/" + PMH_CLIENT_ID)
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is(PMH_CLIENT_ID));
  }

  boolean harvestCompleted(String tenant, String pmhClientId) {
    String response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .get("/meta-storage/pmh-clients/" + pmhClientId + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .extract().body().asString();
    JsonObject res = new JsonObject(response);
    return "idle".equals(res.getJsonArray("items").getJsonObject(0).getString("status"));
  }

  @Test
  public void oaiPmhClientJobs() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is(PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is(PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/stop")
        .then().statusCode(404)
        .contentType("text/plain")
        .body(Matchers.is(PMH_CLIENT_ID));

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", "http://localhost:" + MODULE_PORT + "/meta-storage/oai")
        .put("headers", new JsonObject().put(XOkapiHeaders.TENANT, TENANT_1))
        .put("sourceId", SOURCE_ID_1)
        .put("set", "set1")
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", is(0))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));

        RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/stop")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(is("not running"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", is(1));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/stop")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(is("not running"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .delete("/meta-storage/pmh-clients/" + PMH_CLIENT_ID)
        .then().statusCode(204);
  }

  /**
   * Create records in TENANT1 and harvest them to TENANT2.
   */
  @Test
  public void oaiPmhClientFetch()  {
    createIsbnMatchKey();

    JsonArray records1 = new JsonArray();
    for (int i = 0; i < 10; i++) {
          records1.add(new JsonObject()
              .put("localId", "S" + i)
              .put("payload", new JsonObject()
                  .put("marc", new JsonObject().put("leader", "00914naa  0101   450 "))
                  .put("inventory", new JsonObject().put("isbn", new JsonArray().add(Integer.toString(i))))
              )
          );
    }
    ingestRecords(records1, SOURCE_ID_1);

    /* harvest records from tenant1 and store them in tenant2 */
    JsonObject oaiPmhClient = new JsonObject()
        .put("url", MODULE_URL + "/meta-storage/oai")
        .put("set", "isbn")
        .put("params", new JsonObject().put("limit", "4"))
        .put("headers", new JsonObject().put(XOkapiHeaders.TENANT, TENANT_1))
        .put("sourceId", SOURCE_ID_1)
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_2, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(10))
        .body("items[0].totalRequests", is(3)) // 4 + 4 + 2 : 3 requests with limit 4
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].error", is(nullValue()))
        .body("items[0].config.resumptionToken", is(nullValue()))
        .body("items[0].config.from", hasLength(20))
        .body("items[0].config.until", is(nullValue()))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID)
        .then().statusCode(200)
        .contentType("application/json")
        .body("resumptionToken", is(nullValue()))
        .body("from", hasLength(20))
        .body("until", is(nullValue()))
        .body("sourceId", is(SOURCE_ID_1));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .param("count", "exact")
        .get("/meta-storage/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("resultInfo.totalRecords", is(10));

    records1 = new JsonArray();
    for (int i = 0; i < 3; i++) {
      records1.add(new JsonObject()
              .put("localId", "S" + i)
              .put("delete", true)
          );
    }
    ingestRecords(records1, SOURCE_ID_1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_2, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.resumptionToken", is(nullValue()))
        .body("items[0].config.from", hasLength(20))
        .body("items[0].config.until", is(nullValue()))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_2)
        .param("count", "exact")
        .get("/meta-storage/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("resultInfo.totalRecords", is(7));
  }

  @Test
  public void oaiPmhClientNoServer() {
    createIsbnMatchKey();

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", "http://localhost:" + UNUSED_PORT + "/meta-storage/oai")
        .put("set", "isbn")
        .put("headers", new JsonObject().put(XOkapiHeaders.TENANT, TENANT_1))
        .put("sourceId", SOURCE_ID_1)
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", is(0))
        .body("items[0].error", containsString("localhost"))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));
  }

  @Test
  public void oaiPmhClientConnectionClosed() {
    createIsbnMatchKey();

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", "http://localhost:" + NET_PORT + "/mock/oai")
        .put("set", "isbn")
        .put("sourceId", SOURCE_ID_1)
        .put("waitRetries", 1)
        .put("numberRetries", 1)
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", is(0))
        .body("items[0].error", is("Connection was closed"))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));
  }

  @Test
  public void oaiPmhClientHttpStatus() {
    createIsbnMatchKey();

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", MOCK_URL + "/mock/oai")
        .put("set", "isbn")
        .put("sourceId", SOURCE_ID_1)
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    mockBody = "mock error";
    mockContentType = "text/plain";
    mockStatus = 400;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", is(1))
        .body("items[0].error", containsString("HTTP status 400: mock error"))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));
  }

  @Test
  public void oaiPmhClientHttpBadXml() {
    createIsbnMatchKey();

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", MOCK_URL + "/mock/oai")
        .put("set", "isbn")
        .put("sourceId", SOURCE_ID_1)
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    mockBody = "<foo";
    mockContentType = "text/xml";
    mockStatus = 200;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", is(1))
        .body("items[0].error", is(nullValue())) // error should be reported
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));
  }

  @Test
  public void oaiPmhClientHttpBadMetadata() {
    createIsbnMatchKey();

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", MOCK_URL + "/mock/oai")
        .put("set", "isbn")
        .put("headers", new JsonObject().put(XOkapiHeaders.TENANT, TENANT_1))
        .put("sourceId", SOURCE_ID_1)
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    mockBody = """
<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2022-06-09T09:54:45Z</responseDate>
  <request verb="ListRecords" set="isbn" metadataPrefix="marc21">https://localhost/mock/oai</request>
  <ListRecords><record>
    <metadata><foo/></metadata>
  </record>
  <resumptionToken>MzM5OzE7Ozt2MS4w</resumptionToken></ListRecords>
  </OAI-PMH>
        """;
    mockContentType = "text/xml";
    mockStatus = 200;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", is(1))
        .body("items[0].error", is("Bad marcxml element: foo"))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));

    mockBody = """
<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2022-06-09T09:54:45Z</responseDate>
  <request verb="ListRecords" set="isbn" metadataPrefix="marc21">https://localhost/mock/oai</request>
  <ListRecords>
  <resumptionToken>MzM5OzE7Ozt2MS4w</resumptionToken></ListRecords>
  </OAI-PMH>
        """;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", is(3))
        .body("items[0].error", is(nullValue()))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));
  }

  @Test
  public void oaiPmhClientSameResumptionToken() {
    createIsbnMatchKey();

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", MOCK_URL + "/mock/oai")
        .put("set", "isbn")
        .put("headers", new JsonObject().put(XOkapiHeaders.TENANT, TENANT_1))
        .put("sourceId", SOURCE_ID_1)
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    mockBody = """
<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2022-06-09T09:54:45Z</responseDate>
  <request verb="ListRecords" set="isbn" metadataPrefix="marc21">https://localhost/mock/oai</request>
  <ListRecords>
  <resumptionToken>MzM5OzE7Ozt2MS4w</resumptionToken></ListRecords>
  </OAI-PMH>
        """;
    mockContentType = "text/xml";
    mockStatus = 200;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", is(2))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));
  }


  @Test
  public void oaiPmhClientStop() {
    createIsbnMatchKey();

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/stop")
        .then().statusCode(404)
        .body(is(PMH_CLIENT_ID));

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", MOCK_URL + "/mock/oai")
        .put("set", "isbn")
        .put("headers", new JsonObject().put(XOkapiHeaders.TENANT, TENANT_1))
        .put("sourceId", SOURCE_ID_1)
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    mockBody = """
<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2022-06-09T09:54:45Z</responseDate>
  <request verb="ListRecords" set="isbn" metadataPrefix="marc21">https://localhost/mock/oai</request>
  <ListRecords>
  <resumptionToken>MzM5OzE7Ozt2MS4w</resumptionToken></ListRecords>
  </OAI-PMH>
        """;
    mockContentType = "text/xml";
    mockStatus = 200;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/stop")
        .then().statusCode(400)
        .body(containsString("not running"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/start")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/stop")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/" + PMH_CLIENT_ID + "/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", greaterThanOrEqualTo(1))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));
  }

  @Test
  public void oaiPmhClientAllIllegal() {
    JsonObject oaiPmhClient = new JsonObject()
        .put("url", MOCK_URL + "/mock/oai")
        .put("set", "isbn")
        .put("headers", new JsonObject().put(XOkapiHeaders.TENANT, TENANT_1))
        .put("sourceId", SOURCE_ID_1)
        .put("id", "_all");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(is("Invalid value for OAI PMH client identifier: _all"));
  }

  @Test
  public void oaiPmhClientAll() {
    createIsbnMatchKey();

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/_all/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(0));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/_all/stop")
        .then().statusCode(204);

    JsonObject oaiPmhClient = new JsonObject()
        .put("url", MOCK_URL + "/mock/oai")
        .put("set", "isbn")
        .put("headers", new JsonObject().put(XOkapiHeaders.TENANT, TENANT_1))
        .put("sourceId", SOURCE_ID_1)
        .put("id", PMH_CLIENT_ID);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .header("Content-Type", "application/json")
        .body(oaiPmhClient.encode())
        .post("/meta-storage/pmh-clients")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(oaiPmhClient.encode()));

    mockBody = """
<?xml version="1.0"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2022-06-09T09:54:45Z</responseDate>
  <request verb="ListRecords" set="isbn" metadataPrefix="marc21">https://localhost/mock/oai</request>
  <ListRecords>
  <resumptionToken>MzM5OzE7Ozt2MS4w</resumptionToken></ListRecords>
  </OAI-PMH>
        """;
    mockContentType = "text/xml";
    mockStatus = 200;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/_all/stop")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/_all/start")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .post("/meta-storage/pmh-clients/_all/stop")
        .then().statusCode(204);

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> harvestCompleted(TENANT_1, PMH_CLIENT_ID));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, TENANT_1)
        .get("/meta-storage/pmh-clients/_all/status")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items[0].status", is("idle"))
        .body("items[0].totalRecords", is(0))
        .body("items[0].totalRequests", greaterThanOrEqualTo(1))
        .body("items[0].config.id", is(PMH_CLIENT_ID))
        .body("items[0].config.sourceId", is(SOURCE_ID_1));
  }

}
