package org.folio.metastorage.module;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.metastorage.module.impl.ModuleCacheImpl;

public interface ModuleCache {

  static ModuleCache getInstance() {
    return ModuleCacheImpl.getInstance();
  }

  public Future<Module> lookup(Vertx vertx, String tenant, JsonObject config);

  public Future<Module> lookup(RoutingContext ctx, JsonObject config);

  void purge(String tenant, String id);

  void purgeAll();

}
