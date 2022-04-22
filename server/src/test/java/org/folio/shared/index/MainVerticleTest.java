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
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
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
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.shared.index.api.ResumptionToken;
import org.folio.tlib.postgres.testing.TenantPgPoolContainer;
import org.hamcrest.Matchers;
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
        .param("query","foo=bar" )
        .get("/shared-index/records")
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
        .body(containsString(" does not exist (42P01)"));
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
        .put("params", new JsonObject().put("marc", "$.fields.010.subfields[*].a"))
        .put("update", "ingest");

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
        .body(matchKey.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(containsString("duplicate key value violates unique constraint"));

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
        .body("matchKeys[0].update", is(matchKey.getString("update")))
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
        .body("matchKeys[0].update", is(matchKey.getString("update")))
        .body("resultInfo.totalRecords", is(1));

    matchKey.put("update", "manual");
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(200)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/shared-index/config/matchkeys/" + matchKey.getString("id"))
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
        .body("resultInfo.totalRecords", is(2));

    String res = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + sourceId)
        .get("/shared-index/records")
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
        .param("query", "sourceId=" + UUID.randomUUID())
        .get("/shared-index/records")
        .then().statusCode(200)
        .body("items", hasSize(0))
        .body("resultInfo.totalRecords", is(0));

    for (int idx = 0; idx < records.size(); idx++) {
      JsonObject sharedRecord = records.getJsonObject(idx);
      RestAssured.given()
          .header(XOkapiHeaders.TENANT, tenant1)
          .header("Content-Type", "application/json")
          .param("query", "localId==" + sharedRecord.getString("localId"))
          .get("/shared-index/records")
          .then().statusCode(200)
          .body("items", hasSize(1))
          .body("items[0].localId", is(sharedRecord.getString("localId")))
          .body("items[0].marcPayload.leader", is(sharedRecord.getJsonObject("marcPayload").getString("leader")))
          .body("items[0].inventoryPayload.isbn", is(sharedRecord.getJsonObject("inventoryPayload").getString("isbn")))
          .body("items[0].sourceId", is(sourceId))
          .body("resultInfo.totalRecords", is(1));
    }
  }

  static String verifyOaiResponse(String s, String envelope, List<String> identifiers, int length)
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
    while (xmlStreamReader.hasNext()) {
      int event = xmlStreamReader.next();
      if (event == XMLStreamConstants.START_ELEMENT) {
        level++;
        String elem = xmlStreamReader.getLocalName();
        if (level == 2 && elem == envelope) {
          foundEnvelope = true;
        }
        path = path + "/" + elem;
        if (level == 3 && ("record".equals(elem) || "header".equals(elem))) {
          offset++;
        }
        if (level == 3 && "resumptionToken".equals(elem)) {
          event = xmlStreamReader.next();
          if (event == XMLStreamConstants.CHARACTERS) {
            resumptionToken = xmlStreamReader.getText();
          }
        }
        if ("identifier".equals(elem) && xmlStreamReader.hasNext()) {
          event = xmlStreamReader.next();
          if (event == XMLStreamConstants.CHARACTERS) {
            identifiers.add(xmlStreamReader.getText());
          }
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
    return resumptionToken;
  }

  /**
   * Check that each records in each cluster contains exactly set of localIds.
   * @param s cluster response
   * @param localIds expected localId values for each cluster
   */
  static void verifyClusterResponse(String s, List<String> ... localIds) {
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
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(request.encode())
        .put("/shared-index/records")
        .then().statusCode(200);
  }

  @Test
  public void testMatchKeysIngest() {
    JsonObject matchKey = new JsonObject()
        .put("id", "isbn2")
        .put("method", "jsonpath")
        // update = ingest is the default
        .put("params", new JsonObject().put("inventory", "$.isbn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

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
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("2").add("3")))
        );
    ingestRecords(records1, sourceId1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + sourceId1)
        .get("/shared-index/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + sourceId1)
        .get("/shared-index/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2));

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn2")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .body("items[0].records", hasSize(1))
        .body("items[1].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "matchValue=3")
        .param("matchkeyid", "isbn2")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .body("items[0].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S102"));

    ingestRecords(records1, sourceId1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + sourceId1)
        .get("/shared-index/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn2")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .body("items[0].records", hasSize(1))
        .body("items[1].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/shared-index/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);
  }

  @Test
  public void testMatchKeyIdMissing() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/clusters")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(containsString("Missing parameter matchkeyid"));
  }

  @Test
  public void testMatchKeyIdNotFound() {
    String id = UUID.randomUUID().toString();
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("matchkeyid", id)
        .header("Content-Type", "application/json")
        .get("/shared-index/clusters")
        .then().statusCode(404)
        .contentType("text/plain")
        .body(is("MatchKey " + id + " not found"));
  }

  @Test
  public void testClustersSameKey() throws XMLStreamException {
    JsonObject matchKey = new JsonObject()
        .put("id", "issn")
        .put("method", "jsonpath")
        // update = ingest is the default
        .put("params", new JsonObject().put("inventory", "$.issn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

    String sourceId1 = UUID.randomUUID().toString();
    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject()
                .put("issn", new JsonArray().add("1"))
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject()
                .put("issn", new JsonArray().add("1"))
            )
        )
        .add(new JsonObject()
            .put("localId", "S103")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject()
                .put("issn", new JsonArray().add("1"))
            )
        );
    ingestRecords(records1, sourceId1);

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "issn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101", "S102", "S103"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/shared-index/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/issn")
        .then().statusCode(204);
  }

  @Test
  public void testClustersMove() throws XMLStreamException {

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/clusters")
        .then().statusCode(400);

    JsonObject matchKey1 = new JsonObject()
        .put("id", "isbn")
        .put("method", "jsonpath")
        // update = ingest is the default
        .put("params", new JsonObject().put("inventory", "$.isbn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey1.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey1.encode()));

    JsonObject matchKey2 = new JsonObject()
        .put("id", "issn")
        .put("method", "jsonpath")
        // update = ingest is the default
        .put("params", new JsonObject().put("inventory", "$.issn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey2.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey2.encode()));

    String sourceId1 = UUID.randomUUID().toString();
    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject()
                .put("isbn", new JsonArray().add("1"))
                .put("issn", new JsonArray().add("01"))
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject()
                .put("isbn", new JsonArray().add("2").add("3"))
                .put("issn", new JsonArray().add("01"))
            )
        );
    log.info("phase 1: insert two separate isbn recs, but one with issn");
    ingestRecords(records1, sourceId1);

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "issn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .body("items[0].records", hasSize(2))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101", "S102"));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
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
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "clusterId=" + clusterId)
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .body("items[0].records", hasSize(1))
        .body("items[0].clusterId", is(clusterId))
        .body("items[0].datestamp", is(datestamp));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/clusters/" + clusterId)
        .then().statusCode(200)
        .contentType("application/json")
        .body("records", hasSize(1))
        .body("clusterId", is(clusterId));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/clusters/" + UUID.randomUUID())
        .then().statusCode(404);

    log.info("phase 2: S101 from 1 to 4");
    records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("4")))
        );
    ingestRecords(records1, sourceId1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "issn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .body("items[0].records", hasSize(1))
        .body("items[1].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    log.info("phase 3: S101 from 4 to 3");
    records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  2200337   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("3")))
        );
    ingestRecords(records1, sourceId1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101", "S102"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/shared-index/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/isbn")
        .then().statusCode(204);
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/issn")
        .then().statusCode(204);
  }

  @Test
  public void testDeleteSharedRecords() {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + UUID.randomUUID())
        .delete("/shared-index/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .delete("/shared-index/records")
        .then().statusCode(400)
        .contentType("text/plain")
        .body(is("Must specify query for delete records"));

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

    String sourceId1 = UUID.randomUUID().toString();
    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0101   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("1")))
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0102   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("2")))
        );
    ingestRecords(records1, sourceId1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/records")
        .then().statusCode(200)
        .body("resultInfo.totalRecords", is(2));

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "localId=S102")
        .delete("/shared-index/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/records")
        .then().statusCode(200)
        .body("resultInfo.totalRecords", is(1));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "localId=S101")
        .delete("/shared-index/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/records")
        .then().statusCode(200)
        .body("resultInfo.totalRecords", is(0));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(0));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);
  }

  @Test
  public void testEmptyMatchKeys() {
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

    String sourceId1 = UUID.randomUUID().toString();
    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0101   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("1")))
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0102   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray()))
        )
        .add(new JsonObject()
            .put("localId", "S103")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0102   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray()))
        );
    ingestRecords(records1, sourceId1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .get("/shared-index/records")
        .then().statusCode(200)
        .body("resultInfo.totalRecords", is(3));

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(3))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"), List.of("S103"));

    records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S103")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0102   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("1")))
        );
    ingestRecords(records1, sourceId1);
    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101", "S103"), List.of("S102"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/shared-index/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);
  }

  @Test
  public void testMatchKeysManual() {
    JsonObject matchKey = new JsonObject()
        .put("id", "isbn")
        .put("method", "jsonpath")
        .put("update", "manual")
        .put("params", new JsonObject().put("inventory", "$.isbn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey.encode()));

    String sourceId1 = UUID.randomUUID().toString();
    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0101   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("1")))
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0102   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("2")))
        );
    ingestRecords(records1, sourceId1);

    // populate first time
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/shared-index/config/matchkeys/" + matchKey.getString("id") + "/initialize")
        .then().statusCode(200)
        .contentType("application/json");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "sourceId=" + sourceId1)
        .get("/shared-index/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2));

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .body("items[0].records", hasSize(1))
        .body("items[1].records", hasSize(1))
        .extract().body().asString();
    verifyClusterResponse(s, List.of("S101"), List.of("S102"));

    String sourceId2 = UUID.randomUUID().toString();
    JsonArray records2 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S201")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0201   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("3")))
        )
        .add(new JsonObject()
            .put("localId", "S202")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0202   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("1").add("2").add("3").add("4")))
        )
        .add(new JsonObject()
            .put("localId", "S203")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0203   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("5").add("6")))
        )
        .add(new JsonObject()
            .put("localId", "S204")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0204   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("5")))
        )
        .add(new JsonObject()
            .put("localId", "S205")
            .put("marcPayload", new JsonObject().put("leader", "00914naa  0204   450 "))
            .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add("4")))
        );
    ingestRecords(records2, sourceId2);

    // populate again with both sources
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/shared-index/config/matchkeys/" + matchKey.getString("id") + "/initialize")
        .then().statusCode(200)
        .contentType("application/json");

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("matchkeyid", "isbn")
        .get("/shared-index/clusters")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(2))
        .extract().body().asString();

    verifyClusterResponse(s, List.of("S101", "S102", "S201", "S202", "S205"), List.of("S203", "S204"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "localId=S101 and sourceId=" + sourceId1)
        .get("/shared-index/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(1));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "localId==notfound")
        .get("/shared-index/records")
        .then().statusCode(200)
        .contentType("application/json")
        .body("items", hasSize(0));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/" + matchKey.getString("id"))
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey.encode())
        .put("/shared-index/config/matchkeys/" + matchKey.getString("id") + "/initialize")
        .then().statusCode(404)
        .contentType("text/plain")
        .body(is("MatchKey isbn not found"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("query", "cql.allRecords=true")
        .delete("/shared-index/records")
        .then().statusCode(204);
  }

  @Test
  public void testOaiDiagnostics(TestContext context) {
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"badVerb\">missing verb</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "isbn")
        .param("verb", "noop")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"badVerb\">noop</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "GetRecord")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("error code=\"badArgument\">missing identifier</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "badmetadataprefix")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"cannotDisseminateFormat\">only metadataPrefix &quot;marcxml&quot; supported</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"badArgument\">set &quot;null&quot; not found</error>"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("<error code=\"badArgument\">set &quot;isbn&quot; not found</error>"));
  }

  @Test
  public void testIdentify() throws XMLStreamException, IOException, SAXException {
    vertx.getOrCreateContext().config().put("adminEmail", "admin@indexdata.com");
    List<String> identifiers = new LinkedList<>();
    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "Identify")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "Identify", identifiers, 0);
  }

  @Test
  public void testOaiSimple() throws XMLStreamException, IOException, SAXException {
    JsonObject matchKey1 = new JsonObject()
        .put("id", "isbn")
        .put("method", "jsonpath")
        // update = ingest is the default
        .put("params", new JsonObject().put("inventory", "$.isbn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey1.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey1.encode()));

    List<String> identifiers = new LinkedList<>();
    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 0);

    JsonObject matchKey2 = new JsonObject()
        .put("id", "issn")
        .put("method", "jsonpath")
        // update = ingest is the default
        .put("params", new JsonObject().put("inventory", "$.issn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey2.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey2.encode()));

    String sourceId1 = UUID.randomUUID().toString();
    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject()
                .put("leader", "00914naa  2200337   450 ")
                .put("fields", new JsonArray()
                    .add(new JsonObject()
                        .put("999", new JsonObject()
                            .put("ind1", " ")
                            .put("ind2", " ")
                            .put("subfields", new JsonArray()
                                .add(new JsonObject()
                                    .put("a", "S101a")
                                    .put("b", "S101b")
                                )
                            )
                        )
                    )
                )
            )
            .put("inventoryPayload", new JsonObject()
                .put("isbn", new JsonArray().add("1"))
                .put("issn", new JsonArray().add("01"))
            )
        )
        .add(new JsonObject()
            .put("localId", "S102")
            .put("marcPayload", new JsonObject()
                .put("leader", "00914naa  2200337   450 ")
                .put("fields", new JsonArray()
                    .add(new JsonObject()
                        .put("999", new JsonObject()
                            .put("ind1", " ")
                            .put("ind2", " ")
                            .put("subfields", new JsonArray()
                                .add(new JsonObject()
                                    .put("a", "S102a")
                                    .put("b", "S102b")
                                )
                            )
                        )
                    )
                )
            )
            .put("inventoryPayload", new JsonObject()
                .put("isbn", new JsonArray().add("2").add("3"))
                .put("issn", new JsonArray().add("01"))
            )
        );
    ingestRecords(records1, sourceId1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "issn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "issn")
        .param("verb", "ListIdentifiers")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListIdentifiers", identifiers, 1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "GetRecord")
        .param("metadataPrefix", "marcxml")
        .param("identifier", identifiers.get(0))
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "GetRecord", identifiers, 1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "GetRecord")
        .param("metadataPrefix", "marcxml")
        .param("identifier", UUID.randomUUID().toString())
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("idDoesNotExist"));

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 2);

    JsonArray records2 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S103")
            .put("marcPayload", new JsonObject()
                .put("leader", "00914naa  2200337   450 ")
            )
            .put("inventoryPayload", new JsonObject()
                .put("isbn", new JsonArray().add("1").add("2"))
                .put("issn", new JsonArray().add("02"))
                .put("holdingsRecords", new JsonArray().add(new JsonObject()
                        .put("permanentLocationDeref", "S103")
                    )
                )
            )
        );
    ingestRecords(records2, sourceId1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "isbn")
        .param("verb", "ListRecords")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 2);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("set", "isbn")
        .param("verb", "ListIdentifiers")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListIdentifiers", identifiers, 2);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/shared-index/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/isbn")
        .then().statusCode(204);
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/issn")
        .then().statusCode(204);
  }

  @Test
  public void testOaiDatestamp() throws XMLStreamException, InterruptedException, IOException, SAXException {
    String time0 = Instant.now(Clock.systemUTC()).minusSeconds(1L).truncatedTo(ChronoUnit.SECONDS).toString();
    String time1 = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS).toString();

    JsonObject matchKey1 = new JsonObject()
        .put("id", "isbn")
        .put("method", "jsonpath")
        // update = ingest is the default
        .put("params", new JsonObject().put("inventory", "$.isbn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey1.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey1.encode()));

    List<String> identifiers = new LinkedList<>();
    String s;

    String sourceId1 = UUID.randomUUID().toString();
    JsonArray records1 = new JsonArray()
        .add(new JsonObject()
            .put("localId", "S101")
            .put("marcPayload", new JsonObject()
                .put("leader", "00914naa  2200337   450 ")
            )
            .put("inventoryPayload", new JsonObject()
                .put("isbn", new JsonArray().add("1"))
                .put("holdingsRecords", new JsonArray().add(new JsonObject()
                        .put("permanentLocationDeref", "S101")
                    )
                )
            )
        );
    ingestRecords(records1, sourceId1);
    String time2 = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS).toString();
    TimeUnit.SECONDS.sleep(1);
    String time3 = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS).toString();

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "ListRecords")
        .param("from", time1)
        .param("until", time2)
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 1);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "ListRecords")
        .param("until", time0)
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 0);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "ListRecords")
        .param("from", time3)
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 0);

    ingestRecords(records1, sourceId1);
    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "ListRecords")
        .param("from", time3)
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "ListRecords")
        .param("from", "xxxx")
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .body(containsString("error code=\"badArgument\">bad from"));

    TimeUnit.SECONDS.sleep(1);
    String time4 = Instant.now(Clock.systemUTC()).truncatedTo(ChronoUnit.SECONDS).toString();
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/shared-index/records")
        .then().statusCode(204);

    s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "ListRecords")
        .param("from", time4)
        .param("metadataPrefix", "marcxml")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    verifyOaiResponse(s, "ListRecords", identifiers, 1);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/isbn")
        .then().statusCode(204);
  }

  @Test
  public void testOaiResumptionToken() throws XMLStreamException, InterruptedException, IOException, SAXException {
    JsonObject matchKey1 = new JsonObject()
        .put("id", "isbn")
        .put("method", "jsonpath")
        // update = ingest is the default
        .put("params", new JsonObject().put("inventory", "$.isbn[*]"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .body(matchKey1.encode())
        .post("/shared-index/config/matchkeys")
        .then().statusCode(201)
        .contentType("application/json")
        .body(Matchers.is(matchKey1.encode()));

    String sourceId1 = UUID.randomUUID().toString();
    for (int i = 0; i < 10; i++) {
      JsonArray records1 = new JsonArray()
          .add(new JsonObject()
              .put("localId", "S" + i)
              .put("marcPayload", new JsonObject().put("leader", "00914naa  0101   450 "))
              .put("inventoryPayload", new JsonObject().put("isbn", new JsonArray().add(Integer.toString(i))))
          );
      ingestRecords(records1, sourceId1);
    }
    List<String> identifiers = new LinkedList<>();

    String s = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .param("verb", "ListRecords")
        .param("list-limit", "2")
        .get("/shared-index/oai")
        .then().statusCode(200)
        .contentType("text/xml")
        .extract().body().asString();
    int iter;
    for (iter = 0; iter < 10; iter++) {
      String token = verifyOaiResponse(s, "ListRecords", identifiers, -1);
      if (token == null) {
        break;
      }
      ResumptionToken tokenClass = new ResumptionToken(token);
      Assert.assertEquals("isbn", tokenClass.getSet());
      s = RestAssured.given()
          .header(XOkapiHeaders.TENANT, tenant1)
          .param("verb", "ListRecords")
          .param("list-limit", "2")
          .param("resumptionToken", token)
          .get("/shared-index/oai")
          .then().statusCode(200)
          .contentType("text/xml")
          .extract().body().asString();
    }
    Assert.assertEquals(4, iter);
    Assert.assertEquals(10, identifiers.size());

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .header("Content-Type", "application/json")
        .param("query", "cql.allRecords=true")
        .delete("/shared-index/records")
        .then().statusCode(204);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant1)
        .delete("/shared-index/config/matchkeys/isbn")
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

}
