package org.folio.metastorage.server;

import static org.folio.metastorage.util.EncodeXmlText.encodeXmlText;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.metastorage.module.Module;
import org.folio.metastorage.module.ModuleCache;
import org.folio.metastorage.server.entity.ClusterBuilder;
import org.folio.metastorage.util.JsonToMarcXml;
import org.folio.metastorage.util.MarcInJsonUtil;
import org.folio.okapi.common.HttpResponse;
import org.folio.tlib.util.TenantUtil;

public final class OaiService {
  private static final Logger log = LogManager.getLogger(OaiService.class);

  private OaiService() { }

  static final String OAI_HEADER = """
      <?xml version="1.0" encoding="UTF-8"?>
      <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
               http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
      """;

  static String encodeOaiIdentifier(UUID clusterId) {
    return "oai:" + clusterId.toString();
  }

  static UUID decodeOaiIdentifier(String identifier) {
    int off = identifier.indexOf(':');
    return UUID.fromString(identifier.substring(off + 1));
  }

  static void oaiHeader(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    HttpServerResponse response = ctx.response();
    response.setChunked(true);
    response.setStatusCode(200);
    response.putHeader("Content-Type", "text/xml");
    response.write(OAI_HEADER);
    response.write("  <responseDate>" + Instant.now() + "</responseDate>\n");
    response.write("  <request");
    String verb = Util.getParameterString(params.queryParameter("verb"));
    if (verb != null) {
      response.write(" verb=\"" + encodeXmlText(verb) + "\"");
    }
    response.write(">" + encodeXmlText(ctx.request().absoluteURI()) + "</request>\n");
  }

  static void oaiFooter(RoutingContext ctx) {
    HttpServerResponse response = ctx.response();
    response.write("</OAI-PMH>");
    response.end();
  }

  static Future<Void> get(RoutingContext ctx) {
    return getCheck(ctx).recover(e -> {
      if (!(e instanceof OaiException)) {
        // failedFuture ends up as 400, so we return 500 for this
        // as OAI errors are "user" errors.
        HttpResponse.responseError(ctx, 500, e.getMessage());
        return Future.succeededFuture();
      }
      log.error(e.getMessage(), e);
      oaiHeader(ctx);
      String errorCode = ((OaiException) e).getErrorCode();
      ctx.response().write("  <error code=\"" + errorCode + "\">"
          + encodeXmlText(e.getMessage()) + "</error>\n");
      oaiFooter(ctx);
      return Future.succeededFuture();
    });
  }

  static Future<Void> getCheck(RoutingContext ctx) {
    try {
      RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
      String verb = Util.getParameterString(params.queryParameter("verb"));
      if (verb == null) {
        throw OaiException.badVerb("missing verb");
      }
      String metadataPrefix = Util.getParameterString(params.queryParameter("metadataPrefix"));
      if (metadataPrefix != null && !"marcxml".equals(metadataPrefix)) {
        throw OaiException.cannotDisseminateFormat("only metadataPrefix \"marcxml\" supported");
      }
      switch (verb) {
        case "Identify":
          return identify(ctx);
        case "ListIdentifiers":
          return listRecords(ctx, false);
        case "ListRecords":
          return listRecords(ctx, true);
        case "GetRecord":
          return getRecord(ctx);
        default:
          throw OaiException.badVerb(verb);
      }
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  static Future<Void> identify(RoutingContext ctx) {
    oaiHeader(ctx);
    JsonObject config = ctx.vertx().getOrCreateContext().config();
    HttpServerResponse response = ctx.response();
    response.write("  <Identify>\n");
    response.write("    <repositoryName>");
    response.write(encodeXmlText(
        config.getString("repositoryName", "repositoryName unspecified")));
    response.write("    </repositoryName>\n");
    response.write("    <baseURL>");
    response.write(encodeXmlText(
        config.getString("baseURL", "baseURL unspecified")));
    response.write("    </baseURL>\n");
    response.write("    <protocolVersion>2.0</protocolVersion>\n");
    response.write("    <adminEmail>");
    response.write(encodeXmlText(
        config.getString("adminEmail", "admin@mail.unspecified")));
    response.write("</adminEmail>\n");
    response.write("    <earliestDatestamp>2020-01-01T00:00:00Z</earliestDatestamp>\n");
    response.write("    <deletedRecord>persistent</deletedRecord>\n");
    response.write("    <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>\n");
    response.write("  </Identify>\n");
    oaiFooter(ctx);
    return Future.succeededFuture();
  }

  static Future<Void> listRecords(RoutingContext ctx, boolean withMetadata) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String coded = Util.getParameterString(params.queryParameter("resumptionToken"));
    ResumptionToken token = coded != null ? new ResumptionToken(coded) : null;
    String set = token != null
        ? token.getSet() : Util.getParameterString(params.queryParameter("set"));
    String from = Util.getParameterString(params.queryParameter("from"));
    String until = token != null
        ? token.getUntil() : Util.getParameterString(params.queryParameter("until"));
    Integer limit = params.queryParameter("limit").getInteger();
    Storage storage = new Storage(ctx);
    return storage.selectMatchKeyConfig(set).compose(conf -> {
      if (conf == null) {
        throw OaiException.badArgument("set \"" + set + "\" not found");
      }
      List<Object> tupleList = new ArrayList<>();
      tupleList.add(conf.getString("id"));
      StringBuilder sqlQuery = new StringBuilder("SELECT * FROM " + storage.getClusterMetaTable()
          + " WHERE match_key_config_id = $1");
      int no = 2;
      if (token != null) {
        tupleList.add(token.getFrom()); // from resumptionToken is with fraction of seconds
        sqlQuery.append(" AND datestamp >= $" + no);
        no++;
      } else if (from != null) {
        tupleList.add(Util.parseFrom(from));
        sqlQuery.append(" AND datestamp >= $" + no);
        no++;
      }
      if (until != null) {
        tupleList.add(Util.parseUntil(until));
        sqlQuery.append(" AND datestamp < $" + no);
      }
      ResumptionToken resumptionToken = new ResumptionToken(conf.getString("id"), until);
      sqlQuery.append(" ORDER BY datestamp");
      return getTransformerModule(storage, ctx)
          .compose(module -> storage.getPool().getConnection().compose(conn ->
              listRecordsResponse(ctx, module, storage, conn, sqlQuery.toString(),
                  Tuple.from(tupleList), limit, withMetadata, resumptionToken)
          ));
    });
  }

  private static void endListResponse(RoutingContext ctx, SqlConnection conn, Transaction tx,
      String elem) {
    tx.commit().compose(y -> conn.close());
    HttpServerResponse response = ctx.response();
    if (!response.headWritten()) { // no records returned is an error which is so weird.
      oaiHeader(ctx);
      ctx.response().write("  <error code=\"noRecordsMatch\"/>\n");
    } else {
      response.write("  </" + elem + ">\n");
    }
    oaiFooter(ctx);
  }

  /**
   * Construct metadata record XML string.
   *
   * <p>999 ind1=1 ind2=0 has identifiers for the record. $i cluster UUID; multiple $m for each
   * match value; Multiple $l, $s pairs for local identifier and source identifiers.
   *
   * <p>999 ind1=0 ind2=0 has holding information. Not complete yet.
   *
   * @param clusterJson ClusterBuilder.build output
   * @return metadata record string; null if it's deleted record
   */
  static String getMetadataJava(JsonObject clusterJson) {
    JsonArray identifiersField = new JsonArray();
    identifiersField.add(new JsonObject()
        .put("i", clusterJson.getString(ClusterBuilder.CLUSTER_ID_LABEL)));
    JsonArray matchValues = clusterJson.getJsonArray(ClusterBuilder.MATCH_VALUES_LABEL);
    for (int i = 0; i < matchValues.size(); i++) {
      String matchValue = matchValues.getString(i);
      identifiersField.add(new JsonObject().put("m", matchValue));
    }
    JsonArray records = clusterJson.getJsonArray("records");
    JsonObject combinedMarc = null;
    for (int i = 0; i < records.size(); i++) {
      JsonObject clusterRecord = records.getJsonObject(i);
      JsonObject thisMarc = clusterRecord.getJsonObject(ClusterBuilder.PAYLOAD_LABEL)
          .getJsonObject("marc");
      JsonArray f999 = MarcInJsonUtil.lookupMarcDataField(thisMarc, "999", " ", " ");
      if (combinedMarc == null) {
        combinedMarc = thisMarc;
      } else {
        JsonArray c999 = MarcInJsonUtil.lookupMarcDataField(combinedMarc, "999", " ", " ");
        // normally we'd have 999 in combined record
        if (f999 != null && c999 != null) {
          c999.addAll(f999); // all 999 in one data field
        }
      }
      identifiersField.add(new JsonObject()
          .put("l", clusterRecord.getString(ClusterBuilder.LOCAL_ID_LABEL)));
      identifiersField.add(new JsonObject()
          .put("s", clusterRecord.getString(ClusterBuilder.SOURCE_ID_LABEL)));
    }
    if (combinedMarc == null) {
      return null; // a deleted record
    }
    MarcInJsonUtil.createMarcDataField(combinedMarc, "999", "1", "0").addAll(identifiersField);
    return JsonToMarcXml.convert(combinedMarc);
  }

  static Future<Module> getTransformerModule(Storage storage, RoutingContext ctx) {
    return storage.selectOaiConfig()
        .compose(oaiCfg -> {
          if (oaiCfg == null) {
            return Future.succeededFuture(null);
          }
          String transformer = oaiCfg.getString("transformer");
          if (transformer == null) {
            return Future.succeededFuture(null);
          }
          return storage.selectCodeModuleEntity(transformer)
              .compose(module -> {
                if (module == null) {
                  return Future.failedFuture("Transformer not found: " + transformer);
                }
                return ModuleCache.getInstance().lookup(
                    ctx.vertx(), TenantUtil.tenant(ctx), module.asJson());
              });
        });
  }

  static Future<List<String>> getClusterValues(Storage storage, SqlConnection conn,
      UUID clusterId) {
    return conn.preparedQuery("SELECT match_value FROM " + storage.getClusterValuesTable()
            + " WHERE cluster_id = $1")
        .execute(Tuple.of(clusterId))
        .map(rowSet -> {
          List<String> values = new ArrayList<>();
          rowSet.forEach(row -> values.add(row.getString("match_value")));
          return values;
        });
  }

  static void writeResumptionToken(RoutingContext ctx, ResumptionToken token) {
    HttpServerResponse response = ctx.response();
    response.write("    <resumptionToken>");
    response.write(encodeXmlText(token.encode()));
    response.write("</resumptionToken>\n");
  }

  @java.lang.SuppressWarnings({"squid:S107"})  // too many arguments
  static Future<Void> listRecordsResponse(RoutingContext ctx, Module module, Storage storage,
      SqlConnection conn, String sqlQuery, Tuple tuple, Integer limit, boolean withMetadata,
      ResumptionToken token) {

    String elem = withMetadata ? "ListRecords" : "ListIdentifiers";
    return conn.prepare(sqlQuery).compose(pq ->
        conn.begin().compose(tx -> {
          HttpServerResponse response = ctx.response();
          ClusterRecordStream clusterRecordStream
              = new ClusterRecordStream(ctx.vertx(), storage, conn, response, module, withMetadata);
          RowStream<Row> stream = pq.createStream(100, tuple);
          AtomicInteger cnt = new AtomicInteger();
          clusterRecordStream.drainHandler(x -> stream.resume());
          stream.handler(row -> {
            if (cnt.get() == 0) {
              oaiHeader(ctx);
              response.write("  <" + elem + ">\n");
            }
            LocalDateTime datestamp = row.getLocalDateTime("datestamp");
            if (token.getFrom() == null || datestamp.isAfter(token.getFrom())) {
              token.setFrom(datestamp);
              if (cnt.get() >= limit) {
                stream.pause();
                clusterRecordStream.end().onComplete(y -> {
                  writeResumptionToken(ctx, token);
                  endListResponse(ctx, conn, tx, elem);
                });
                return;
              }
            }
            cnt.incrementAndGet();
            ClusterRecordItem cr = new ClusterRecordItem();
            cr.clusterId = row.getUUID("cluster_id");
            cr.datestamp = datestamp;
            cr.oaiSet = row.getString("match_key_config_id");
            clusterRecordStream.write(cr);
            if (clusterRecordStream.writeQueueFull()) {
              stream.pause();
            }
          });
          stream.endHandler(end ->
              clusterRecordStream.end()
                  .onComplete(y -> endListResponse(ctx, conn, tx, elem))
          );
          stream.exceptionHandler(e -> {
            log.error("stream error {}", e.getMessage(), e);
            endListResponse(ctx, conn, tx, elem);
          });
          return Future.succeededFuture();
        })
    );
  }

  static Future<Void> getRecord(RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String identifier = Util.getParameterString(params.queryParameter("identifier"));
    if (identifier == null) {
      throw OaiException.badArgument("missing identifier");
    }
    UUID clusterId = decodeOaiIdentifier(identifier);
    Storage storage = new Storage(ctx);
    return getTransformerModule(storage, ctx).compose(module -> {
      String sqlQuery = "SELECT * FROM " + storage.getClusterMetaTable() + " WHERE cluster_id = $1";
      return storage.getPool()
          .withConnection(conn -> conn.preparedQuery(sqlQuery)
              .execute(Tuple.of(clusterId))
              .compose(res -> {
                RowIterator<Row> iterator = res.iterator();
                if (!iterator.hasNext()) {
                  throw OaiException.idDoesNotExist(identifier);
                }
                Row row = iterator.next();
                ClusterRecordItem cr = new ClusterRecordItem();
                cr.clusterId = row.getUUID("cluster_id");
                cr.datestamp = row.getLocalDateTime("datestamp");
                cr.oaiSet = row.getString("match_key_config_id");
                HttpServerResponse response = ctx.response();
                ClusterRecordStream clusterRecordStream
                    = new ClusterRecordStream(ctx.vertx(), storage, conn, response, module, true);
                return clusterRecordStream.getClusterRecordMetadata(cr)
                    .map(buf -> {
                      oaiHeader(ctx);
                      response.write("  <GetRecord>\n");
                      response.write(buf);
                      response.write("  </GetRecord>\n");
                      oaiFooter(ctx);
                      return null;
                    });
              }));
    });
  }
}
