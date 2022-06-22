package org.folio.metastorage.module.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.HashMap;
import java.util.Map;
import org.folio.metastorage.module.Module;
import org.folio.metastorage.module.ModuleCache;
import org.folio.tlib.util.TenantUtil;

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
      if (module == null) {
        throw new IllegalArgumentException("Arg module must not be null");
      }
      if (config == null) {
        throw new IllegalArgumentException("Arg config must not be null");
      }
      this.module = module;
      this.config = config;
    }

  }

  @Override
  public Future<Module> lookup(RoutingContext ctx, JsonObject config) {
    return lookup(ctx.vertx(), TenantUtil.tenant(ctx), config);
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
    try {
      return module.initialize(vertx, config).map(x -> {
        CacheEntry e = new CacheEntry(module, config);
        entries.put(cacheKey, e);
        return module;
      });
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public void purge(String tenantId, String moduleId) {
    String cacheKey = tenantId + ":" + moduleId;
    CacheEntry entry = entries.get(cacheKey);
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
