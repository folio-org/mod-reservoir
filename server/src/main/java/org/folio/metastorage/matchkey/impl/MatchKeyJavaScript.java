package org.folio.metastorage.matchkey.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import java.util.Collection;
import org.folio.metastorage.matchkey.MatchKeyMethod;
import org.folio.okapi.common.WebClientFactory;
import org.graalvm.polyglot.Value;

public class MatchKeyJavaScript implements MatchKeyMethod {

  Value getKeysFunction;

  org.graalvm.polyglot.Context context;

  Future<Value> evalUrl(Vertx vertx, String url) {
    WebClient webClient = WebClientFactory.getWebClient(vertx);
    return webClient.getAbs(url)
        .expect(ResponsePredicate.SC_OK)
        .send()
        .map(response -> context.eval("js", response.bodyAsString()));
  }

  @Override
  public Future<Void> configure(Vertx vertx, JsonObject configuration) {
    context = org.graalvm.polyglot.Context.create("js");
    Future<Value> future = Future.succeededFuture(null);
    String url = configuration.getString("url");
    if (url != null) {
      future = evalUrl(vertx, url);
    }
    return future.map(value -> {
      String script = configuration.getString("script");
      if (script != null) {
        getKeysFunction = context.eval("js", script);
      } else if (value != null) {
        getKeysFunction = value;
      } else {
        throw new IllegalArgumentException("javascript: url or script must be given");
      }
      return null;
    });
  }

  private void addValue(Collection<String> keys, Value value) {
    if (value.isNumber()) {
      keys.add(Long.toString(value.asLong()));
    } else if (value.isString()) {
      keys.add(value.asString());
    }
  }

  @Override
  public void getKeys(JsonObject payload, Collection<String> keys) {
    Value value = getKeysFunction.execute(payload.encode());
    if (value.hasArrayElements()) {
      for (int i = 0; i < value.getArraySize(); i++) {
        Value memberValue = value.getArrayElement(i);
        addValue(keys, memberValue);
      }
    } else {
      addValue(keys, value);
    }
  }

  @Override
  public void close() {
    if (context != null) {
      context.close(true);
      context = null;
    }
  }
}
