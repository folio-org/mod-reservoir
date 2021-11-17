# [OpenAPI](https://www.openapis.org/)

## Status

 * Current version [3.1.0](https://spec.openapis.org/oas/v3.1.0)
   released 15 February 2021

 * First release to support JSON schemas. For 3.0 it wasn't really JSON schemas:
   https://swagger.io/docs/specification/data-models/keywords/
   Properties like `$schema`, `id`, `$id` not supported.

## Tools

 * Many tools available: https://openapi.tools/
 * Vert.x Web API Service  https://vertx.io/docs/vertx-web-api-service/java/
 * [OpenAPI generator](https://openapi-generator.tech/)

## Vertx.x Web API usage

 * OpenAPI 3.0
 * Library, *not* framework
 * Resource, file or remote URL for spec
 * Multiple specs with overlapping prefixes: yes.
 * Some validation
 * Handler based (id), no interfaces
 * Ease of use: yes
 * Error reporting: so so
 * Streaming requests: Requests: no, but it may be coded separatedly without breaking the model.
 * Streaming responses: Yes. Regular routing context web API.

## OpenAPI Generator

 * OpenAPI 3.0, but it seems to allow regular JSON schemas
 * Template based with many languages supported
 * Framework
 * API generated at compile time with openapi-generator-maven-plugin
 * Multiple specs.. invoke plugin multiple times
 * Code generated with `java-vertx-web` is very similar to Vert.X Web





