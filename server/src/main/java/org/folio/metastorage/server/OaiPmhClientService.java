package org.folio.metastorage.server;

import io.netty.handler.codec.http.QueryStringEncoder;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.metastorage.oai.OaiParserStream;
import org.folio.metastorage.oai.OaiRecord;
import org.folio.metastorage.util.SourceId;
import org.folio.metastorage.util.XmlMetadataParserMarcInJson;
import org.folio.metastorage.util.XmlMetadataStreamParser;
import org.folio.metastorage.util.XmlParser;
import org.folio.okapi.common.HttpResponse;

public class OaiPmhClientService {

  Vertx vertx;

  HttpClient httpClient;

  private static final String STATUS_LITERAL = "status";

  private static final String RESUMPTION_TOKEN_LITERAL = "resumptionToken";

  private static final String IDLE_LITERAL = "idle";

  private static final String RUNNING_LITERAL = "running";

  private static final String CONFIG_LITERAL = "config";

  private static final String TOTAL_RECORDS_LITERAL = "totalRecords";

  private static final String TOTAL_REQUESTS_LITERAL = "totalRequests";

  private static final String B_WHERE_ID1_LITERAL = " WHERE ID = $1";

  private static final Logger log = LogManager.getLogger(OaiPmhClientService.class);

  public OaiPmhClientService(Vertx vertx) {
    this.vertx = vertx;
    this.httpClient = vertx.createHttpClient();
  }

  /**
   * Create OAI-PMH client.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> post(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    JsonObject config = ctx.getBodyAsJson();

    String id = config.getString("id");
    config.remove("id");
    return storage.getPool().preparedQuery("INSERT INTO " + storage.getOaiPmhClientTable()
            + " (id, config)"
            + " VALUES ($1, $2)")
        .execute(Tuple.of(id, config))
        .map(x -> {
          HttpResponse.responseJson(ctx, 201).end(config.put("id", id).encode());
          return null;
        });
  }

  static Future<Row> getOaiPmhClient(Storage storage, SqlConnection connection, String id) {
    return connection.preparedQuery("SELECT * FROM " + storage.getOaiPmhClientTable()
            + B_WHERE_ID1_LITERAL)
        .execute(Tuple.of(id))
        .map(rowSet -> {
          RowIterator<Row> iterator = rowSet.iterator();
          if (!iterator.hasNext()) {
            return null;
          }
          return iterator.next();
        });
  }

  static Future<JsonObject> getConfig(Storage storage, String id) {
    return storage.getPool().withConnection(connection ->
        getOaiPmhClient(storage, connection, id).map(row -> {
          if (row == null) {
            return null;
          }
          return row.getJsonObject(CONFIG_LITERAL);
        }));
  }

  static Future<JsonObject> getJob(Storage storage, String id) {
    return storage.getPool().withConnection(connection -> getJob(storage, connection, id));
  }

  static Future<JsonObject> getJob(Storage storage, SqlConnection connection, String id) {
    return getOaiPmhClient(storage, connection, id).map(row -> {
      if (row == null) {
        return null;
      }
      JsonObject job = row.getJsonObject("job");
      if (job == null) {
        job = new JsonObject()
            .put(STATUS_LITERAL, IDLE_LITERAL)
            .put(TOTAL_RECORDS_LITERAL, 0L)
            .put(TOTAL_REQUESTS_LITERAL, 0L);
      }
      JsonObject config = job.getJsonObject(CONFIG_LITERAL);
      if (config == null) {
        config = row.getJsonObject(CONFIG_LITERAL);
      }
      config.put("id", id);
      job.put(CONFIG_LITERAL, config);
      return job;
    });
  }

  /**
   * Get OAI-PMH client.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> get(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    return getConfig(storage, id).map(config -> {
      if (config == null) {
        HttpResponse.responseError(ctx, 404, id);
        return null;
      }
      config.put("id", id);
      HttpResponse.responseJson(ctx, 200).end(config.encode());
      return null;
    });
  }

  /**
   * Get all OAI-PMH clients.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> getCollection(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    return storage.getPool().query("SELECT id,config FROM " + storage.getOaiPmhClientTable())
        .execute()
        .map(rowSet -> {
          JsonArray ar = new JsonArray();
          rowSet.forEach(x -> {
            JsonObject config = x.getJsonObject(CONFIG_LITERAL);
            config.put("id", x.getValue("id"));
            ar.add(config);
          });
          JsonObject response = new JsonObject();
          response.put("items", ar);
          response.put("resultInfo", new JsonObject().put(TOTAL_RECORDS_LITERAL, ar.size()));
          HttpResponse.responseJson(ctx, 200).end(response.encode());
          return null;
        });
  }

  /**
   * Delete OAI-PMH client.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> delete(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    return storage.getPool().preparedQuery("DELETE FROM " + storage.getOaiPmhClientTable()
            + B_WHERE_ID1_LITERAL)
        .execute(Tuple.of(id))
        .map(rowSet -> {
          if (rowSet.rowCount() == 0) {
            HttpResponse.responseError(ctx, 404, id);
          } else {
            ctx.response().setStatusCode(204).end();
          }
          return null;
        });
  }

  /**
   * Update OAI-PMH client.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> put(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    JsonObject config = ctx.getBodyAsJson();
    config.remove("id");
    return updateJob(storage, id, config, null, null, null)
        .map(found -> {
          if (Boolean.TRUE.equals(found)) {
            ctx.response().setStatusCode(204).end();
          } else {
            HttpResponse.responseError(ctx, 404, id);
          }
          return null;
        });
  }

  static Future<Boolean> updateJob(
      Storage storage, String id,
      JsonObject config, JsonObject job, Boolean stop, UUID owner) {
    return storage.getPool()
        .withConnection(connection -> updateJob(storage, connection, id, config, job, stop, owner));
  }

  static Future<Boolean> updateJob(Storage storage, SqlConnection connection, String id,
      JsonObject config, JsonObject job, Boolean stop, UUID owner) {

    StringBuilder qry = new StringBuilder("UPDATE " + storage.getOaiPmhClientTable() + " SET ");
    List<Object> tupleList = new LinkedList<>();
    tupleList.add(id);
    if (config != null) {
      tupleList.add(config);
      qry.append("config = $" + tupleList.size());
    }
    if (job != null) {
      tupleList.add(job);
      if (tupleList.size() > 2) {
        qry.append(",");
      }
      qry.append("job = $" + tupleList.size());
    }
    if (stop != null) {
      tupleList.add(stop);
      if (tupleList.size() > 2) {
        qry.append(",");
      }
      qry.append("stop = $" + tupleList.size());
    }
    if (owner != null) {
      tupleList.add(owner);
      if (tupleList.size() > 2) {
        qry.append(",");
      }
      qry.append("owner = $" + tupleList.size());
    }
    qry.append(B_WHERE_ID1_LITERAL);
    return connection.preparedQuery(qry.toString())
        .execute(Tuple.from(tupleList))
        .map(rowSet -> rowSet.rowCount() > 0);
  }

  /**
   * Start OAI PMH client job.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> start(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));

    return getJob(storage, id).compose(job -> {
      if (job == null) {
        return Future.succeededFuture(null);
      }
      job.put(STATUS_LITERAL, RUNNING_LITERAL);
      UUID owner = UUID.randomUUID();
      return updateJob(storage, id, null, job, Boolean.FALSE, owner)
          .map(job)
          .onSuccess(x -> oaiHarvestLoop(storage, id, job, owner));
    }).map(job -> {
      if (job == null) {
        HttpResponse.responseError(ctx, 404, id);
      } else {
        ctx.response().setStatusCode(204).end();
      }
      return null;
    });
  }

  Future<Row> getStopOwner(Storage storage, String id) {
    return storage.getPool().preparedQuery("SELECT stop, owner FROM "
            + storage.getOaiPmhClientTable() + B_WHERE_ID1_LITERAL).execute(Tuple.of(id))
        .map(rowSet -> {
          RowIterator<Row> iterator = rowSet.iterator();
          if (!iterator.hasNext()) {
            return null; // probably removed elsewhere ... so stop
          }
          return iterator.next();
        });
  }

  /**
   * Stop OAI PMH client job.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> stop(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));

    return getJob(storage, id)
        .compose(job -> {
          if (job == null) {
            HttpResponse.responseError(ctx, 404, id);
            return Future.succeededFuture();
          }
          String status = job.getString(STATUS_LITERAL);
          if (IDLE_LITERAL.equals(status)) {
            HttpResponse.responseError(ctx, 400, "not running");
            return Future.succeededFuture();
          }
          return updateJob(storage, id, null, null, Boolean.TRUE, null)
              .map(x -> {
                ctx.response().setStatusCode(204).end();
                return null;
              });
        });
  }

  /**
   * Get OAI PMH client status.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> status(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));

    return storage.pool.withConnection(connection ->
        getJob(storage, connection, id)
            .map(job -> {
              if (job == null) {
                HttpResponse.responseError(ctx, 404, id);
                return null;
              }
              HttpResponse.responseJson(ctx, 200).end(job.encode());
              return null;
            }));
  }

  static MultiMap getHttpHeaders(JsonObject config) {
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("Accept", "text/xml");
    JsonObject userHeaders = config.getJsonObject("headers");
    if (userHeaders != null) {
      userHeaders.forEach(e -> {
        if (e.getValue() instanceof String value) {
          headers.add(e.getKey(), value);
        } else {
          throw new IllegalArgumentException("headers " + e.getKey() + " value must be string");
        }
      });
    }
    return headers;
  }

  static boolean addQueryParameterFromConfig(QueryStringEncoder enc,
      JsonObject config, String key) {
    String value = config.getString(key);
    if (value == null) {
      return false;
    }
    enc.addParam(key, value);
    return true;
  }

  static void addQueryParameterFromParams(QueryStringEncoder enc, JsonObject params) {
    if (params != null) {
      params.forEach(e -> {
        if (e.getValue() instanceof String value) {
          enc.addParam(e.getKey(), value);
        } else {
          throw new IllegalArgumentException("params " + e.getKey() + " value must be string");
        }
      });
    }
  }

  Future<Void> ingestRecord(
      Storage storage, OaiRecord<JsonObject> oaiRecord,
      SourceId sourceId, JsonArray matchKeyConfigs) {
    try {
      JsonObject globalRecord = new JsonObject();
      globalRecord.put("localId", oaiRecord.getIdentifier());
      if (oaiRecord.isDeleted()) {
        globalRecord.put("delete", true);
      } else {
        globalRecord.put("payload", new JsonObject().put("marc", oaiRecord.getMetadata()));
      }
      return storage.ingestGlobalRecord(vertx, sourceId, globalRecord, matchKeyConfigs);
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  Future<HttpClientResponse> listRecordsRequest(JsonObject config) {
    RequestOptions requestOptions = new RequestOptions();
    requestOptions.setMethod(HttpMethod.GET);
    requestOptions.setHeaders(getHttpHeaders(config));
    QueryStringEncoder enc = new QueryStringEncoder(config.getString("url"));
    enc.addParam("verb", "ListRecords");
    if (!addQueryParameterFromConfig(enc, config, RESUMPTION_TOKEN_LITERAL)) {
      addQueryParameterFromConfig(enc, config, "from");
      addQueryParameterFromConfig(enc, config, "until");
      addQueryParameterFromConfig(enc, config, "set");
      addQueryParameterFromConfig(enc, config, "metadataPrefix");
    }
    addQueryParameterFromParams(enc, config.getJsonObject("params"));
    String absoluteUri = enc.toString();
    requestOptions.setAbsoluteURI(absoluteUri);
    return httpClient.request(requestOptions).compose(HttpClientRequest::send);
  }

  private Future<Void> listRecordsResponse(Storage storage, JsonObject job, JsonObject config,
      JsonArray matchKeyConfigs, HttpClientResponse res) {
    job.put(TOTAL_REQUESTS_LITERAL, job.getLong(TOTAL_REQUESTS_LITERAL) + 1);
    if (res.statusCode() != 200) {
      Promise<Void> promise = Promise.promise();
      Buffer buffer = Buffer.buffer();
      res.handler(buffer::appendBuffer);
      res.exceptionHandler(promise::tryFail);
      res.endHandler(end -> {
        String msg = buffer.length() > 80
            ? buffer.getString(0, 80) : buffer.toString();
        promise.fail("Returned HTTP status " + res.statusCode() + ": " + msg);
      });
      return promise.future();
    }
    XmlParser xmlParser = XmlParser.newParser(res);
    XmlMetadataStreamParser<JsonObject> metadataParser
        = new XmlMetadataParserMarcInJson();
    SourceId sourceId = new SourceId(config.getString("sourceId"));
    AtomicInteger cnt = new AtomicInteger();
    Promise<Void> promise = Promise.promise();
    OaiParserStream<JsonObject> oaiParserStream =
        new OaiParserStream<>(xmlParser,
            oaiRecord -> {
              cnt.incrementAndGet();
              // populate from with newest datestamp from all responses
              String datestamp = oaiRecord.getDatestamp();
              String from = config.getString("from");
              if (from == null || datestamp.compareTo(from) > 0) {
                config.put("from", datestamp);
              }
              xmlParser.pause();
              ingestRecord(storage, oaiRecord, sourceId, matchKeyConfigs)
                  .onComplete(x -> xmlParser.resume());
            },
            metadataParser);
    oaiParserStream.exceptionHandler(promise::fail);
    xmlParser.endHandler(end -> {
      job.put(TOTAL_RECORDS_LITERAL, job.getLong(TOTAL_RECORDS_LITERAL) + cnt.get());
      String resumptionToken = oaiParserStream.getResumptionToken();
      String oldResumptionToken = config.getString(RESUMPTION_TOKEN_LITERAL);
      if (resumptionToken == null
          || resumptionToken.equals(oldResumptionToken)) {
        config.remove(RESUMPTION_TOKEN_LITERAL);
        promise.fail((String) null);
      } else {
        config.put(RESUMPTION_TOKEN_LITERAL, resumptionToken);
        promise.complete();
      }
    });
    return promise.future();
  }

  void oaiHarvestLoop(Storage storage, String id, JsonObject job, UUID owner) {
    JsonObject config = job.getJsonObject(CONFIG_LITERAL);
    log.info("harvest loop id={} owner={}", id, owner);
    getStopOwner(storage, id)
        .onSuccess(row -> {
          if (!row.getUUID("owner").equals(owner)) {
            return;
          }
          Future<Void> f;
          job.remove("error");
          if (Boolean.TRUE.equals(row.getBoolean("stop"))) {
            f = Future.failedFuture((String) null);
          } else {
            f = storage.getAvailableMatchConfigs()
                .compose(matchKeyConfigs ->
                    listRecordsRequest(config)
                        .compose(res ->
                            listRecordsResponse(storage, job, config, matchKeyConfigs, res)));
          }
          f.compose(x ->
                  // continue harvesting
                  updateJob(storage, id, config, job, null, null)
                      // only continue if we can also save job
                      .onSuccess(x1 -> oaiHarvestLoop(storage, id, job, owner))
              )
              .onFailure(e -> {
                // stop either due to error or no resumption token or "stop"
                job.put(STATUS_LITERAL, IDLE_LITERAL);
                if (e.getMessage() != null) {
                  log.error(e.getMessage(), e);
                  job.put("error", e.getMessage());
                }
                // hopefully updateJob works so that error can be saved.
                updateJob(storage, id, config, job, null, null);
              });
        });
  }
}
