package org.folio.reshare.index.api;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.nio.file.Path;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;

/**
 * Tests that shaded jar and Dockerfile work.
 */
public class ApiIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiIT.class);

  private static final Network network = Network.newNetwork();

  @ClassRule
  public static final GenericContainer<?> module =
    new GenericContainer<>(
        new ImageFromDockerfile("mod-reshare-index").withFileFromPath(".", Path.of(".")))
      .withNetwork(network)
      .withNetworkAliases("module")
      .withExposedPorts(8081)
      .withEnv("DB_HOST", "postgres")
      .withEnv("DB_PORT", "5432")
      .withEnv("DB_USERNAME", "username")
      .withEnv("DB_PASSWORD", "password")
      .withEnv("DB_DATABASE", "postgres");

  @Rule
  public final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:12-alpine")
    .withNetwork(network)
    .withNetworkAliases("postgres")
    .withExposedPorts(5432)
    .withUsername("username")
    .withPassword("password")
    .withDatabaseName("postgres");

  @Rule
  public final MockServerContainer okapi =
    new MockServerContainer(DockerImageName.parse("mockserver/mockserver:mockserver-5.11.2"))
      .withNetwork(network)
      .withNetworkAliases("okapi")
      .withExposedPorts(1080);

  @Before
  public void before() {
    module.followOutput(new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams());
    RestAssured.baseURI = "http://" + module.getHost() + ":" + module.getFirstMappedPort();

    var mockServerClient = new MockServerClient(okapi.getHost(), okapi.getServerPort());
    mockServerClient.when(request().withMethod("GET"))
      .respond(response().withStatusCode(200).withBody("{}", MediaType.JSON_UTF_8));

    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.requestSpecification = new RequestSpecBuilder()
      .addHeader("x-okapi-tenant", "testtenant")
      .addHeader("x-okapi-url", "http://okapi:1080")
      .setContentType(ContentType.JSON)
      .build();
  }

  @Test
  public void health() {
    when().
      get("/admin/health").
    then().
      statusCode(200).
      body(is("OK"));
  }

  private void postTenant() {
    String location =
        given().
          body("{ \"module_to\": \"99.99.99\" }").
        when().
          post("/_/tenant").
        then().
          statusCode(201).
        extract().
          header("Location");

    when().
      get(location + "?wait=30").
    then().
      statusCode(200).
      body("complete", is(true));
  }

  //@Test
  public void useOverTime() {
    postTenant();
    given().
      param("full", "true").
    when().
      get("/reshare-index/shared-titles").
    then().
      statusCode(200);
  }


}
