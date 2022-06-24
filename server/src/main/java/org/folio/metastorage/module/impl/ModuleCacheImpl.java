package org.folio.metastorage.module.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;
import org.folio.metastorage.module.Module;
import org.folio.metastorage.module.ModuleCache;

public class ModuleCacheImpl implements ModuleCache {

  private static class LazyInstance {
    static final ModuleCacheImpl instance = new ModuleCacheImpl();
  }

  public static ModuleCache getInstance() {
    return LazyInstance.instance;
  }

  private Map<String, CacheEntry> entries = new HashMap<>();

  private ModuleCacheImpl() { }

  private class CacheEntry {
    private final Module module;
    private final JsonObject config;

    CacheEntry(Module module, JsonObject config) {
      this.module = module;
      this.config = config;
    }

  }

  @Override
  public Future<Module> lookup(Vertx vertx, String tenantId, JsonObject config) {
    String moduleId = config.getString("id");
    if (moduleId == null) {
      return Future.failedFuture("module config must include 'id'");
    }
    String cacheKey = tenantId + ":" + moduleId;
    CacheEntry entry = entries.get(cacheKey);
    if (entry != null) {
      if (entry.config.equals(config)) {
        return Future.succeededFuture(entry.module);
      }
      entry.module.terminate();
      entries.remove(cacheKey);
    }
    Module module = new EsModuleImpl();
    return module.initialize(vertx, config).map(x -> {
      CacheEntry e = new CacheEntry(module, config);
      entries.put(cacheKey, e);
      return module;
    });
  }

  @Override
  public void purge(String tenantId, String moduleId) {
    String cacheKey = tenantId + ":" + moduleId;
    CacheEntry entry = entries.remove(cacheKey);
    if (entry != null) {
      entry.module.terminate();
    }
  }

  @Override
  public void purgeAll() {
    entries.forEach((x,y) -> y.module.terminate());
    entries.clear();
  }

}
