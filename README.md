# Reservoir

Copyright (C) 2021-2022 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

## Introduction

A service that provides a clustering storage of metadata for the purpose of consortial re-sharing. Optimized for fast storage and retrieval performance.

This project has three sub-projects:

* `util` -- A library with utilities to normalize MARC to Inventory.
* `server` -- The reservoir storage server. This is the FOLIO module: mod-reservoir
* `client` -- A client for sending ISO2709/MARCXML records to the server.

## Compilation

Requirements:

* Java 17. A later version might very well work
* Maven 3.6.3 or later
* `JAVA_HOME` set, e.g.\
   `export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:bin/javac::")`

Install all components with: `mvn install`

## Server

Start the server with:
```
java -Dport=8081 --module-path=server/target/compiler/ \
  --upgrade-module-path=server/target/compiler/compiler.jar \
  -XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI \
  -jar server/target/mod-reservoir-server-fat.jar
```

The module is configured by setting environment variables:
`DB_HOST`, `DB_PORT`, `DB_USERNAME`, `DB_PASSWORD`, `DB_DATABASE`,
`DB_MAXPOOLSIZE`, `DB_SERVER_PEM`.

## Command-line client

The client is a command-line tool for sending records to the mod-reservoir server.

Run the client with:
```
java -jar client/target/mod-reservoir-client-fat.jar [options] [files...]
```

To see list options use `--help`. The client uses environment variables
`OKAPI_URL`, `OKAPI_TENANT`, `OKAPI_TOKEN` for Okapi URL, tenant and
token respectively.

Before records can be pushed, the database needs to be prepared for the tenant.
If Okapi is used, then the usual `install` command will do it, but if the
mod-reservoir module is being run on its own, then that must be done manually.

For example, to prepare the database for tenant `diku` on server running on localhost:8081, use:
```
export OKAPI_TENANT=diku
export OKAPI_URL=http://localhost:8081
java -jar client/target/mod-reservoir-client-fat.jar --init
```

To purge the data, use:
```
export OKAPI_TENANT=diku
export OKAPI_URL=http://localhost:8081
java -jar client/target/mod-reservoir-client-fat.jar --purge
```

To send MARCXML to the same server with defined `sourceId`, use:
```
export OKAPI_TENANT=diku
export OKAPI_URL=http://localhost:8081
export sourceid=lib1
java -jar client/target/mod-reservoir-client-fat.jar \
  --source $sourceid \
  --xsl xsl/marc2inventory-instance.xsl \
  --xsl xsl/holdings-items-cst.xsl \
  --xsl xsl/library-codes-cst.xsl \
  client/src/test/resources/record10.xml
```

The option `--xsl` may be repeated for a sequence of transformations.

## OAI-PMH client

The OAI-PMH client is executing in the server. So it is not an external client.
Commands are sent to the server to initiate the client operations.

### OAI-PMH client configuration

The OAI-PMH client is configured by posting simple JSON configuration.
The identifier `id` is user-defined and given in the initial post.
Example with identifier `us-mdbj` below:

```
export OKAPI_TENANT=diku
export OKAPI_URL=http://localhost:8081
cat oai-us-mdbj.json
{
  "id": "us-mdbj",
  "set": "397",
  "sourceId": "US-MDBJ",
  "url": "https://pod.stanford.edu/oai",
  "metadataPrefix": "marc21",
  "headers": {
    "Authorization": "Bearer ey.."
  }
}
curl -HX-Okapi-Tenant:$OKAPI_TENANT -HContent-Type:application/json -XPOST \
   -d@oai-us-mdbj.json $OKAPI_URL/reservoir/pmh-clients
```

In this case, all ingested records from the client are given the source identifier `US-MDBJ`.

See [schema](server/src/main/resources/openapi/schemas/oai-pmh-client.json) for more information.

This configuration can be inspected with:
```
curl -HX-Okapi-Tenant:$OKAPI_TENANT \
  $OKAPI_URL/reservoir/pmh-clients/us-mdbj
```

Start a job with:
```
curl -HX-Okapi-Tenant:$OKAPI_TENANT -XPOST \
  $OKAPI_URL/reservoir/pmh-clients/us-mdbj/start
```

Start all jobs with:
```
curl -HX-Okapi-Tenant:$OKAPI_TENANT -XPOST \
  $OKAPI_URL/reservoir/pmh-clients/_all/start
```

Each job will continue until the server returns error or returns no resumption token. The `from`
property of the configuration is populated with latest datestamp in records received. This enables
the client to repeat the job again at a later date to fetch updates from `from` to now (unless `until` is
specified).

Get status for a job with:
```
curl -HX-Okapi-Tenant:$OKAPI_TENANT \
  $OKAPI_URL/reservoir/pmh-clients/us-mdbj/status
```

Get status for all jobs with:
```
curl -HX-Okapi-Tenant:$OKAPI_TENANT \
  $OKAPI_URL/reservoir/pmh-clients/_all/status
```

Stop a job with:
```
curl -HX-Okapi-Tenant:$OKAPI_TENANT -XPOST \
  $OKAPI_URL/reservoir/pmh-clients/us-mdbj/stop
```

Stop all jobs with:
```
curl -HX-Okapi-Tenant:$OKAPI_TENANT -XPOST \
  $OKAPI_URL/reservoir/pmh-clients/_all/stop
```

**Note**: The abovementioned commands are for the server running on localhost.
For a real server, the `-HX-Okapi-Token:$OKAPI_TOKEN` is required.

## Additional information

### Issue tracker

See project [RSRVR](https://issues.folio.org/browse/RSRVR)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

### Code of Conduct

Refer to the Wiki [FOLIO Code of Conduct](https://wiki.folio.org/display/COMMUNITY/FOLIO+Code+of+Conduct).

### ModuleDescriptor

See the [ModuleDescriptor](descriptors/ModuleDescriptor-template.json)
for the interfaces that this module requires and provides, the permissions,
and the additional module metadata.

### API documentation

API descriptions:

 * [OpenAPI](server/src/main/resources/openapi/)
 * [Schemas](server/src/main/resources/openapi/schemas/)

Generated [API documentation](https://dev.folio.org/reference/api/#mod-reservoir).

### Code analysis

[SonarQube analysis](https://sonarcloud.io/dashboard?id=org.folio%3Amod-reservoir).

### Download and configuration

The built artifacts for this module are available.
See [configuration](https://dev.folio.org/download/artifacts) for repository access,
and the Docker images for [released versions](https://hub.docker.com/r/folioorg/mod-reservoir/)
and for [snapshot versions](https://hub.docker.com/r/folioci/mod-reservoir/).

