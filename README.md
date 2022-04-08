# mod-shared-index

Copyright (C) 2021-2022 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

## Introduction

A service that generates an index of bibliographic records and items for the purpose of consortial re-sharing.
The index is made as simple as possible in order to be optimized for fast loading.

This projects has three sub projects:

 * util: a library with utilities to normalize MARC to Inventory.
 * server: The shared index server. This is FOLIO module: mod-shared-index.
 * client: A client for sending ISO2709/MARCXML records to the server.

### Compilation

You need:

 * Java 11. A later version might very well work
 * Maven 3.6.3 or later
 * `JAVA_HOME` set. Can be set with
   `export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")`

Install all components with:

    mvn install

### Server

Start the server with

    java -Dport=8081 -jar server/target/mod-shared-index-server-fat.jar

The module is configured by setting environment variables:
`DB_HOST`, `DB_PORT`, `DB_USERNAME`, `DB_DATABASE`, `DB_MAXPOOLSIZE`, `DB_SERVER_PEM`.

### Client

Run the client with

    java -jar client/target/mod-shared-index-client-fat.jar [options] [files...]

To see list options use `--help`. The client uses environment variables
`OKAPI_URL`, `OKAPI_TENANT`, `OKAPI_TOKEN` for Okapi URL, tenant and
token respectively.

Before you can push record, you'll want to prepare the database for the
tenant. If Okapi is used, then the install command will do it, but if you
are running mod-shared-index module on its own, you must do that manually.

For example, to prepare database for tenant `diku` on server running on localhost:8081, use:

    export OKAPI_TENANT=diku
    export OKAPI_URL=http://localhost:8081
    java -jar client/target/mod-shared-index-client-fat.jar --init

Should you want to purge the data, use:

    export OKAPI_TENANT=diku
    export OKAPI_URL=http://localhost:8081
    java -jar client/target/mod-shared-index-client-fat.jar --purge

To send MARCXML to the same server with defined sourceId:

    export OKAPI_TENANT=diku
    export OKAPI_URL=http://localhost:8081
    export sourceid=`uuidgen`
    java -jar client/target/mod-shared-index-client-fat.jar \
      --source $sourceid \
      --xsl xsl/marc2inventory-instance.xsl \
      --xsl xsl/holdings-items-cst.xsl \
      --xsl xsl/library-codes-cst.xsl \
      client/src/test/resources/record10.xml

The option `--xsl` may be repeated for a sequence of transformations.

## Additional information

### Issue tracker

See project [MODSI](https://issues.folio.org/browse/MODSI)
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

Generated [API documentation](https://dev.folio.org/reference/api/#mod-shared-index).

### Code analysis


### Download and configuration


