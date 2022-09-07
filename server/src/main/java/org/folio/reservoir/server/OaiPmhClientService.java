package org.folio.reservoir.server;

import io.netty.handler.codec.http.QueryStringEncoder;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
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
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.HttpResponse;
import org.folio.reservoir.server.entity.ClusterBuilder;
import org.folio.reservoir.server.entity.OaiPmhStatus;
import org.folio.reservoir.util.SourceId;
import org.folio.reservoir.util.XmlMetadataParserMarcInJson;
import org.folio.reservoir.util.XmlMetadataStreamParser;
import org.folio.reservoir.util.XmlParser;
import org.folio.reservoir.util.oai.OaiParserStream;
import org.folio.reservoir.util.oai.OaiRecord;

public class OaiPmhClientService {

  Vertx vertx;

  HttpClient httpClient;

  private static final String RESUMPTION_TOKEN_LITERAL = "resumptionToken";

  private static final String CONFIG_LITERAL = "config";

  private static final String B_WHERE_ID1_LITERAL = " WHERE ID = $1";

  private static final String CLIENT_ID_ALL = "_all";

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
    if (CLIENT_ID_ALL.equals(id)) {
      return Future.failedFuture("Invalid value for OAI PMH client identifier: " + id);
    }
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

  static Future<RowSet<Row>> getOaiPmhClients(Storage storage) {
    return storage.getPool().query("SELECT * FROM " + storage.getOaiPmhClientTable())
        .execute();
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

  static Future<OaiPmhStatus> getJob(Storage storage, String id) {
    return storage.getPool().withConnection(connection -> getJob(storage, connection, id));
  }

  static Future<OaiPmhStatus> getJob(Storage storage, SqlConnection connection, String id) {
    return getOaiPmhClient(storage, connection, id).map(row -> getJob(row, id));
  }

  static OaiPmhStatus getJob(Row row, String id) {
    if (row == null) {
      return null;
    }
    JsonObject json = row.getJsonObject("job");
    OaiPmhStatus oaiPmhStatus;
    if (json != null) {
      // earlier version of mod-reservoir stored config in jobs (not anymore)
      json.remove(CONFIG_LITERAL);
      oaiPmhStatus = json.mapTo(OaiPmhStatus.class);
    } else {
      oaiPmhStatus = new OaiPmhStatus();
      oaiPmhStatus.setStatusIdle();
      oaiPmhStatus.setTotalRecords(0L);
      oaiPmhStatus.setTotalRequests(0);
    }
    // because these fields did not exist in earlier mod-reservoir versions.
    if (oaiPmhStatus.getTotalDeleted() == null) {
      oaiPmhStatus.setTotalDeleted(0L);
    }
    if (oaiPmhStatus.getTotalInserted() == null) {
      oaiPmhStatus.setTotalInserted(0L);
    }
    if (oaiPmhStatus.getTotalUpdated() == null) {
      oaiPmhStatus.setTotalUpdated(0L);
    }
    JsonObject config = row.getJsonObject(CONFIG_LITERAL);
    config.put("id", id);
    oaiPmhStatus.setConfig(config);
    return oaiPmhStatus;
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
    return getOaiPmhClients(storage)
        .map(rowSet -> {
          JsonArray ar = new JsonArray();
          rowSet.forEach(x -> {
            JsonObject config = x.getJsonObject(CONFIG_LITERAL);
            config.put("id", x.getValue("id"));
            ar.add(config);
          });
          JsonObject response = new JsonObject();
          response.put("items", ar);
          response.put("resultInfo", new JsonObject().put("totalRecords", ar.size()));
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
    return getJob(storage, id).compose(existing -> {
      if (existing != null) {
        JsonObject existingConfig = existing.getConfig();
        int existingVersion = existingConfig.getInteger(ClusterBuilder.SOURCE_VERSION_LABEL, 1);
        int newVersion = config.getInteger(ClusterBuilder.SOURCE_VERSION_LABEL, 1);
        String existingSourceId = existingConfig.getString(ClusterBuilder.SOURCE_ID_LABEL, "");
        String newSourceId = config.getString(ClusterBuilder.SOURCE_ID_LABEL, "");
        String existingSet = existingConfig.getString("set", "");
        String newSet = config.getString("set", "");
        // increment if same source ID and version *and* the set is changed.
        if (existingVersion == newVersion && existingSourceId.equals(newSourceId)
            && !existingSet.equals(newSet)) {
          config.put(ClusterBuilder.SOURCE_VERSION_LABEL, existingVersion + 1);
          config.remove("from");
        }
      }
      return updateJob(storage, id, config, null, null, null)
          .map(found -> {
            if (Boolean.TRUE.equals(found)) {
              ctx.response().setStatusCode(204).end();
            } else {
              HttpResponse.responseError(ctx, 404, id);
            }
            return null;
          });
    });
  }

  static Future<Boolean> updateJob(
      Storage storage, String id,
      JsonObject config, OaiPmhStatus job, Boolean stop, UUID owner) {
    return storage.getPool()
        .withConnection(connection -> updateJob(storage, connection, id, config, job, stop, owner));
  }

  static Future<Boolean> updateJob(Storage storage, SqlConnection connection, String id,
      JsonObject config, OaiPmhStatus job, Boolean stop, UUID owner) {

    StringBuilder qry = new StringBuilder("UPDATE " + storage.getOaiPmhClientTable() + " SET ");
    List<Object> tupleList = new LinkedList<>();
    tupleList.add(id);
    if (config != null) {
      tupleList.add(config);
      qry.append("config = $");
      qry.append(tupleList.size());
    }
    if (job != null) {
      tupleList.add(JsonObject.mapFrom(job));
      if (tupleList.size() > 2) {
        qry.append(",");
      }
      qry.append("job = $");
      qry.append(tupleList.size());
    }
    if (stop != null) {
      tupleList.add(stop);
      if (tupleList.size() > 2) {
        qry.append(",");
      }
      qry.append("stop = $");
      qry.append(tupleList.size());
    }
    if (owner != null) {
      tupleList.add(owner);
      if (tupleList.size() > 2) {
        qry.append(",");
      }
      qry.append("owner = $");
      qry.append(tupleList.size());
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
    Future<Boolean> future;
    if (CLIENT_ID_ALL.equals(id)) {
      future = getOaiPmhClients(storage)
          .map(rowSet -> {
            List<Future<Void>> futures = new LinkedList<>();
            rowSet.forEach(x -> {
              String id2 = x.getString("id");
              OaiPmhStatus job = getJob(x, id2);
              futures.add(startJob(storage, id2, job));
            });
            return GenericCompositeFuture.all(futures);
          })
          .map(true);
    } else {
      future = getJob(storage, id)
          .compose(job -> {
            if (job == null) {
              return Future.succeededFuture(false);
            }
            return startJob(storage, id, job).map(true);
          });
    }
    return future
        .onSuccess(x -> {
          if (Boolean.TRUE.equals(x)) {
            ctx.response().setStatusCode(204).end();
          } else {
            HttpResponse.responseError(ctx, 404, id);
          }
        }).mapEmpty();
  }

  Future<Void> startJob(Storage storage, String id, OaiPmhStatus job) {
    job.setStatusRunning();
    job.setLastTotalRecords(0L);
    job.setLastRecsPerSec(null);
    job.setLastStartedTimestampRaw(LocalDateTime.now(ZoneOffset.UTC));
    UUID owner = UUID.randomUUID();
    return updateJob(storage, id, null, job, Boolean.FALSE, owner)
        .onSuccess(x -> oaiHarvestLoop(storage, id, job, owner, 0))
        .mapEmpty();
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

    if (CLIENT_ID_ALL.equals(id)) {
      return getOaiPmhClients(storage)
          .map(rowSet -> {
            List<Future<Void>> futures = new LinkedList<>();
            rowSet.forEach(x -> {
              String id2 = x.getString("id");
              OaiPmhStatus job = getJob(x, id2);
              if (job.isRunning()) {
                futures.add(updateJob(storage, id2, null, null, Boolean.TRUE, null).mapEmpty());
              }
            });
            return GenericCompositeFuture.all(futures);
          })
          .onSuccess(x -> ctx.response().setStatusCode(204).end())
          .mapEmpty();
    }
    return getJob(storage, id)
        .compose(job -> {
          if (job == null) {
            HttpResponse.responseError(ctx, 404, id);
            return Future.succeededFuture();
          }
          if (!job.isRunning()) {
            HttpResponse.responseError(ctx, 400, "not running");
            return Future.succeededFuture();
          }
          return updateJob(storage, id, null, null, Boolean.TRUE, null)
              .onSuccess(x -> ctx.response().setStatusCode(204).end())
              .mapEmpty();
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
    Future<JsonArray> f;
    if (CLIENT_ID_ALL.equals(id)) {
      f = getOaiPmhClients(storage)
          .map(rowSet -> {
            JsonArray items = new JsonArray();
            rowSet.forEach(row -> {
              String id2 = row.getString("id");
              items.add(getJob(row, id2).getJsonObject());
            });
            return items;
          });
    } else {
      f = getJob(storage, id).map(job -> job == null
          ? null : new JsonArray().add(job.getJsonObject()));
    }
    return f
        .onSuccess(items -> {
          if (items == null) {
            HttpResponse.responseError(ctx, 404, id);
            return;
          }
          JsonObject response = new JsonObject();
          response.put("items", items);
          HttpResponse.responseJson(ctx, 200).end(response.encode());
        })
        .mapEmpty();
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

  Future<Boolean> ingestRecord(
      Storage storage, OaiRecord<JsonObject> oaiRecord,
      SourceId sourceId, int sourceVersion, JsonArray matchKeyConfigs) {
    try {
      JsonObject globalRecord = new JsonObject();
      globalRecord.put("localId", oaiRecord.getIdentifier());
      if (oaiRecord.isDeleted()) {
        globalRecord.put("delete", true);
      } else {
        globalRecord.put("payload", new JsonObject().put("marc", oaiRecord.getMetadata()));
      }
      return storage.ingestGlobalRecord(vertx, sourceId, sourceVersion,
          globalRecord, matchKeyConfigs);
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

  static void endResponse(StringBuilder resumptionToken, Promise<Void> promise, OaiPmhStatus job) {
    JsonObject config = job.getConfig();
    LocalDateTime started = job.getLastStartedTimestampRaw();
    long runningTimeMilli =
        Duration.between(started, job.getLastActiveTimestampRaw()).toMillis();
    job.setLastRunningTime(runningTimeMilli);
    job.calculateLastRecsPerSec();
    String oldResumptionToken = config.getString(RESUMPTION_TOKEN_LITERAL);
    if (resumptionToken.length() == 0
        || resumptionToken.toString().equals(oldResumptionToken)) {
      moveFromDate(config);
      config.remove(RESUMPTION_TOKEN_LITERAL);
      promise.fail((String) null);
    } else {
      config.put(RESUMPTION_TOKEN_LITERAL, resumptionToken);
      promise.complete();
    }
  }

  /**
   * If the last datestamp is at least 1 DAY or 1 HOUR before "now"
   * we bump it by 1 DAY or 1 SEC respectively, to avoid reharvesting
   * the same files next time.
   * @param config job config
   */
  static void moveFromDate(JsonObject config) {
    String from = config.getString("from");
    if (from != null &&  Util.unitsBetween(Util.getOaiNow(), from) < 0) {
      config.put("from", Util.getNextOaiDate(from));
    }
  }

  private Future<Void> listRecordsResponse(Storage storage, OaiPmhStatus job,
      JsonArray matchKeyConfigs, HttpClientResponse res) {
    job.setTotalRequests(job.getTotalRequests() + 1);
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
    JsonObject config = job.getConfig();
    SourceId sourceId = new SourceId(config.getString("sourceId"));
    Promise<Void> promise = Promise.promise();
    AtomicInteger queue = new AtomicInteger();
    AtomicBoolean ended = new AtomicBoolean();
    int sourceVersion = config.getInteger("sourceVersion", 1);
    StringBuilder resumptionToken = new StringBuilder();
    OaiParserStream<JsonObject> oaiParserStream =
        new OaiParserStream<>(xmlParser,
            oaiRecord -> {
              // populate from with newest datestamp from all responses
              String datestamp = oaiRecord.getDatestamp();
              String from = config.getString("from");
              if (from == null || datestamp.compareTo(from) > 0) {
                config.put("from", datestamp);
              }
              queue.incrementAndGet();
              // is queue full: 4 because that's the Postgres client pool size
              if (Boolean.FALSE.equals(ended.get()) && queue.get() >= 4) {
                xmlParser.pause();
              }
              ingestRecord(storage, oaiRecord, sourceId, sourceVersion,
                  matchKeyConfigs)
                  .map(upd -> {
                    queue.decrementAndGet();
                    // drain ?
                    if (queue.get() < 2) {
                      xmlParser.resume();
                    }
                    job.setTotalRecords(job.getTotalRecords() + 1);
                    job.setLastTotalRecords(job.getLastTotalRecords() + 1);
                    if (upd == null) {
                      job.setTotalDeleted(job.getTotalDeleted() + 1);
                    } else if (Boolean.TRUE.equals(upd)) {
                      job.setTotalInserted(job.getTotalInserted() + 1);
                    } else {
                      job.setTotalUpdated(job.getTotalUpdated() + 1);
                    }
                    if (queue.get() == 0 && Boolean.TRUE.equals(ended.get())) {
                      endResponse(resumptionToken, promise, job);
                    }
                    return null;
                  })
                  .onFailure(x -> xmlParser.resume());
            },
            metadataParser);
    oaiParserStream.exceptionHandler(promise::fail);
    xmlParser.endHandler(end -> {
      ended.set(true);
      String tmp = oaiParserStream.getResumptionToken();
      if (tmp != null) {
        resumptionToken.append(tmp);
      }
      if (queue.get() == 0) {
        endResponse(resumptionToken, promise, job);
      }
    });
    return promise.future();
  }

  void oaiHarvestLoop(Storage storage, String id, OaiPmhStatus job, UUID owner, int retries) {
    JsonObject config = job.getConfig();
    log.info("harvest loop id={} owner={}", id, owner);
    getStopOwner(storage, id)
        .onSuccess(row -> {
          if (!row.getUUID("owner").equals(owner)) {
            return;
          }
          job.setError(null);
          job.setLastActiveTimestampRaw(LocalDateTime.now(ZoneOffset.UTC));
          Future<Integer> f;
          if (Boolean.TRUE.equals(row.getBoolean("stop"))) {
            f = Future.failedFuture((String) null);
          } else {
            f = storage.getAvailableMatchConfigs()
                .compose(matchKeyConfigs ->
                    listRecordsRequest(config)
                        .compose(res ->
                            listRecordsResponse(storage, job, matchKeyConfigs, res)))
                .map(0)
                .recover(e -> {
                  if (e instanceof VertxException && "Connection was closed".equals(e.getMessage())
                      && retries < config.getInteger("numberRetries", 3)) {
                    Promise<Integer> promise = Promise.promise();
                    long w = config.getInteger("waitRetries", 10);
                    vertx.setTimer(1000 * w, x -> promise.complete(retries + 1));
                    return promise.future();
                  }
                  return Future.failedFuture(e);
                });
          }
          f.compose(newRetries ->
                  // continue harvesting
                  updateJob(storage, id, config, job, null, null)
                      // only continue if we can also save job
                      .onSuccess(x1 -> oaiHarvestLoop(storage, id, job, owner, newRetries))
              )
              .onFailure(e -> {
                // stop either due to error or no resumption token or "stop"
                job.setStatusIdle();
                if (e.getMessage() != null) {
                  log.error(e.getMessage(), e);
                  job.setError(e.getMessage());
                }
                // hopefully updateJob works so that error can be saved.
                updateJob(storage, id, config, job, null, null);
              });
        });
  }
}
