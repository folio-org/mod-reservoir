package org.folio.reshare.index;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.tlib.postgres.testing.TenantPgPoolContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {
  private final static Logger log = LogManager.getLogger("MainVerticleTest");

  static Vertx vertx;
  static final int MODULE_PORT = 9230;
  static final int MOCK_PORT = 9231;
  static final String TENANT = "testlib";

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  static JsonObject getIndexMock() {
    JsonObject indexMock = new JsonObject();
    return indexMock;
  }

  static void getSharedTitles (RoutingContext routingContext) {
    routingContext.response().setChunked(true);
    routingContext.response().putHeader("Content-Type", "application/json");
    routingContext.response().end(getIndexMock().encode());
  }

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.baseURI = "http://localhost:" + MODULE_PORT;
    RestAssured.requestSpecification = new RequestSpecBuilder().build();

    Router router = Router.router(vertx);
    router.getWithRegex("/reshare-index/shared-titles").handler(MainVerticleTest::getSharedTitles);
    vertx.createHttpServer()
        .requestHandler(router)
        .listen(MOCK_PORT)
        .compose(x -> {
          DeploymentOptions deploymentOptions = new DeploymentOptions();
          deploymentOptions.setConfig(new JsonObject().put("port", Integer.toString(MODULE_PORT)));
          return vertx.deployVerticle(new MainVerticle(), deploymentOptions);
        })
        .onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testAdminHealth() {
    RestAssured.given()
            .get("/admin/health")
            .then().statusCode(200)
            .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testGetSharedTitles() {
    RestAssured.given()
            .header(XOkapiHeaders.TENANT, TENANT)
            .get("/reshare-index/shared-titles")
            .then().statusCode(400)
            .header("Content-Type", is("text/plain"))
            .body(is("getSharedTitles: not implemented"));
  }

  @Test
  public void putSharedTitle() {
    JsonObject sharedTitle = new JsonObject()
            .put("localIdentifier", "HRID00121")
            .put("libraryId", "diku911")
            .put("source", new JsonObject())
            .put("inventory", new JsonObject().put("instance", new JsonObject()));
    RestAssured.given()
            .header(XOkapiHeaders.TENANT, TENANT)
            .header("Content-Type", "application/json")
            .body(sharedTitle.encode())
            .put("/reshare-index/shared-titles")
            .then().statusCode(400)
            .header("Content-Type", is("text/plain"))
            .body(is("ERROR: relation \"testlib_mod_reshare_index.bib_record\" does not exist (42P01)"));
  }

}
