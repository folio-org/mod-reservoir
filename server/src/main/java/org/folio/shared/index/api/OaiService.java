package org.folio.shared.index.api;

import static org.folio.shared.index.api.Util.parseFrom;
import static org.folio.shared.index.api.Util.parseUntil;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.shared.index.storage.Storage;
import org.folio.shared.index.util.XmlJsonUtil;

public final class OaiService {
  private static final Logger log = LogManager.getLogger(OaiService.class);

  private OaiService() { }

  static final String OAI_HEADER =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"\n"
          + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
          + "         xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/\n"
          + "         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\">\n";

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
      response.write(" verb=\"" + XmlJsonUtil.encodeXmlText(verb) + "\"");
    }
    response.write(">" + XmlJsonUtil.encodeXmlText(ctx.request().absoluteURI()) + "</request>\n");
  }

  static void oaiFooter(RoutingContext ctx) {
    HttpServerResponse response = ctx.response();
    response.write("</OAI-PMH>");
    response.end();
  }

  static Future<Void> get(RoutingContext ctx) {
    return getCheck(ctx).recover(e -> {
      if (!(e instanceof OaiException)) {
        return Future.failedFuture(e);
      }
      log.error(e.getMessage(), e);
      oaiHeader(ctx);
      String errorCode = ((OaiException) e).getErrorCode();
      ctx.response().write("  <error code=\"" + errorCode + "\">"
          + XmlJsonUtil.encodeXmlText(e.getMessage()) + "</error>\n");
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
    response.write(XmlJsonUtil.encodeXmlText(
        config.getString("repositoryName", "repositoryName unspecified")));
    response.write("    </repositoryName>\n");
    response.write("    <baseURL>");
    response.write(XmlJsonUtil.encodeXmlText(
        config.getString("baseURL", "baseURL unspecified")));
    response.write("    </baseURL>\n");
    response.write("    <protocolVersion>2.0</protocolVersion>\n");
    response.write("    <adminEmail>");
    response.write(XmlJsonUtil.encodeXmlText(
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
    Integer limit = params.queryParameter("list-limit").getInteger();
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
        tupleList.add(token.getFrom()); // from from resumptionToken is with fraction of seconds
        sqlQuery.append(" AND datestamp >= $" + no);
        no++;
      } else if (from != null) {
        tupleList.add(parseFrom(from));
        sqlQuery.append(" AND datestamp >= $" + no);
        no++;
      }
      if (until != null) {
        tupleList.add(parseUntil(until));
        sqlQuery.append(" AND datestamp < $" + no);
      }
      ResumptionToken resumptionToken = new ResumptionToken(conf.getString("id"), until);
      sqlQuery.append(" ORDER BY datestamp");
      return storage.getPool().getConnection().compose(conn ->
          listRecordsResponse(ctx, storage, conn, sqlQuery.toString(), Tuple.from(tupleList),
              limit, withMetadata, resumptionToken)
      );
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
   * @param rowSet bib_record rowSet (empty if no record entries: deleted)
   * @param clusterId cluster identifier that this record is part of
   * @param matchValues match values for this cluster
   * @return
   */
  static String getMetadata(RowSet<Row> rowSet, UUID clusterId, List<String> matchValues) {
    RowIterator<Row> iterator = rowSet.iterator();
    JsonObject combinedMarc = null;
    JsonArray identifiersField = new JsonArray();
    identifiersField.add(new JsonObject().put("i", clusterId.toString()));
    for (String matchValue : matchValues) {
      identifiersField.add(new JsonObject().put("m", matchValue));
    }
    while (iterator.hasNext()) {
      Row row = iterator.next();
      JsonObject thisMarc = row.getJsonObject("marc_payload");
      JsonArray f999 = XmlJsonUtil.lookupMarcDataField(thisMarc, "999", " ", " ");
      if (combinedMarc == null) {
        combinedMarc = thisMarc;
      } else {
        JsonArray c999 = XmlJsonUtil.lookupMarcDataField(combinedMarc, "999", " ", " ");
        // normally we'd have 999 in combined record
        if (f999 != null && c999 != null) {
          c999.addAll(f999); // all 999 in one data field
        }
      }
      identifiersField.add(new JsonObject()
          .put("l", row.getString("local_id"))
          .put("s", row.getUUID("source_id").toString())
      );
    }
    if (combinedMarc == null) {
      return null; // a deleted record
    }
    XmlJsonUtil.createMarcDataField(combinedMarc, "999", "1", "0").addAll(identifiersField);
    String xmlMetadata = XmlJsonUtil.convertJsonToMarcXml(combinedMarc);
    return "    <metadata>\n" + xmlMetadata + "\n    </metadata>\n";
  }

  static Future<String> getXmlRecordMetadata(Storage storage, SqlConnection conn, UUID clusterId,
      List<String> matchValues) {
    String q = "SELECT * FROM " + storage.getBibRecordTable()
        + " LEFT JOIN " + storage.getClusterRecordTable() + " ON record_id = id "
        + " WHERE cluster_id = $1";
    return conn.preparedQuery(q)
        .execute(Tuple.of(clusterId))
        .map(rowSet -> getMetadata(rowSet, clusterId, matchValues));
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

  static Future<String> getXmlRecord(Storage storage, SqlConnection conn, UUID clusterId,
      LocalDateTime datestamp, String oaiSet, boolean withMetadata) {
    Future<List<String>> clusterValues = Future.succeededFuture(Collections.emptyList());
    if (withMetadata) {
      clusterValues = getClusterValues(storage, conn, clusterId);
    }
    // When false withMetadata could optimize and not join with bibRecordTable
    String begin = withMetadata ? "    <record>\n" : "";
    String end = withMetadata ? "    </record>\n" : "";
    return clusterValues.compose(values -> getXmlRecordMetadata(storage, conn, clusterId,
        values)
        .map(metadata ->
            begin
                + "      <header" + (metadata == null ? " status=\"deleted\"" : "") + ">\n"
                + "        <identifier>"
                + XmlJsonUtil.encodeXmlText(encodeOaiIdentifier(clusterId)) + "</identifier>\n"
                + "        <datestamp>"
                + XmlJsonUtil.encodeXmlText(Util.formatOaiDateTime(datestamp))
                + "</datestamp>\n"
                + "        <setSpec>" + XmlJsonUtil.encodeXmlText(oaiSet) + "</setSpec>\n"
                + "      </header>\n"
                + (withMetadata && metadata != null ? metadata : "")
                + end));
  }

  static void writeResumptionToken(RoutingContext ctx, ResumptionToken token) {
    HttpServerResponse response = ctx.response();
    response.write("    <resumptionToken>");
    response.write(XmlJsonUtil.encodeXmlText(token.encode()));
    response.write("</resumptionToken>\n");
  }

  @java.lang.SuppressWarnings({"squid:S107"})  // too many arguments
  static Future<Void> listRecordsResponse(RoutingContext ctx, Storage storage, SqlConnection conn,
      String sqlQuery, Tuple tuple, Integer limit, boolean withMetadata, ResumptionToken token) {
    String elem = withMetadata ? "ListRecords" : "ListIdentifiers";

    return conn.prepare(sqlQuery).compose(pq ->
        conn.begin().compose(tx -> {
          HttpServerResponse response = ctx.response();
          RowStream<Row> stream = pq.createStream(100, tuple);
          AtomicInteger cnt = new AtomicInteger();
          stream.handler(row -> {
            stream.pause();
            if (cnt.get() == 0) {
              oaiHeader(ctx);
              response.write("  <" + elem + ">\n");
            }
            LocalDateTime datestamp = row.getLocalDateTime("datestamp");
            if (token.getFrom() == null || datestamp.isAfter(token.getFrom())) {
              token.setFrom(datestamp);
              if (cnt.get() >= limit) {
                writeResumptionToken(ctx, token);
                stream.close();
                endListResponse(ctx, conn, tx, elem);
                return;
              }
            }
            cnt.incrementAndGet();
            getXmlRecord(storage, conn, row.getUUID("cluster_id"), datestamp,
                row.getString("match_key_config_id"), withMetadata)
                .onSuccess(xmlRecord ->
                    response.write(xmlRecord).onComplete(x -> stream.resume())
                )
                .onFailure(e -> {
                  log.info("failure {}", e.getMessage(), e);
                  stream.close();
                  conn.close();
                });
          });
          stream.endHandler(end -> endListResponse(ctx, conn, tx, elem));
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
              return getXmlRecord(storage, conn,
                  row.getUUID("cluster_id"), row.getLocalDateTime("datestamp"),
                  row.getString("match_key_config_id"), true)
                  .map(xmlRecord -> {
                    oaiHeader(ctx);
                    ctx.response().write("  <GetRecord>\n");
                    ctx.response().write(xmlRecord);
                    ctx.response().write("  </GetRecord>\n");
                    oaiFooter(ctx);
                    return null;
                  });
            }));
  }
}
