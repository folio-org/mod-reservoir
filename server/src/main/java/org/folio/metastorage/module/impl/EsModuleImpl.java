package org.folio.metastorage.module.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import org.folio.metastorage.module.Module;
import org.folio.okapi.common.WebClientFactory;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

public class EsModuleImpl implements Module {

  private String functionName;
  private String url;
  private Value function;
  private Context context;

  @Override
  public Future<Void> initialize(Vertx vertx, JsonObject config) {
    url = config.getString("url");
    if (url == null || url.isEmpty()) {
      return Future.failedFuture(
        new IllegalArgumentException("Module config must include 'url'"));
    }
    functionName = config.getString("function");
    if (functionName == null || functionName.isEmpty()) {
      return Future.failedFuture(
        new IllegalArgumentException("Module config must include 'function'"));
    }
    // only allow ES modules
    final boolean isModule = url.endsWith("mjs");
    if (!isModule) {
      return Future.failedFuture(new IllegalArgumentException(
        "url must end with .mjs to designate ES module"));
    }
    Context.Builder cb = Context.newBuilder("js")
        .allowExperimentalOptions(true)
        .option("js.esm-eval-returns-exports", "true");
    context = cb.build();
    return evalUrl(vertx, url)
      .map(value -> {
        Value v = value.getMember(functionName);
        if (v == null || !v.canExecute()) {
          throw new IllegalArgumentException(
            "Module " + url + " does not include function " + functionName);
        }
        function = v;
        return null;
      });
  }

  private Future<Value> evalUrl(Vertx vertx, String url) {
    WebClient webClient = WebClientFactory.getWebClient(vertx);
    String moduleName = url.substring(url.lastIndexOf("/") + 1);
    return webClient.getAbs(url)
        .expect(ResponsePredicate.SC_OK)
        .send()
        .map(response -> context.eval(Source
          .newBuilder("js", response.bodyAsString(), moduleName)
          .buildLiteral()));
  }

  @Override
  public Future<JsonObject> execute(JsonObject input) {
    Value output = function.execute(input.encode());
    if (output.isString()) {
      //only support string encoded JSON objects for now
      try {
        return Future.succeededFuture(new JsonObject(output.asString()));
      } catch (DecodeException de) {
        return Future.failedFuture(de);
      }
    } else {
      return Future.failedFuture(
        "Function " + functionName + " of module " + url + " must return JSON string");
    }
  }

  @Override
  public Future<Void> terminate() {
    if (context != null) {
      context.close(true);
      context = null;
    }
    return Future.succeededFuture();
  }

}
