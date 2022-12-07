package org.folio.reservoir.server;

import com.jayway.jsonpath.InvalidPathException;
import io.vertx.core.http.HttpServerRequest;
import org.folio.reservoir.module.impl.ModuleJsonPath;
import org.folio.reservoir.util.SourceId;

public class IngestParams {
  final SourceId sourceId;
  final Integer sourceVersion;
  final String contentType;
  final ModuleJsonPath jsonPath;
  final boolean ingest;
  final boolean raw;
  final boolean xmlFixing;
  final String fileName;

  /**
   * Create ingest params from request.
   * @param request request
   */
  public IngestParams(HttpServerRequest request) {
    this.sourceId = new SourceId(validateSourceId(request));
    sourceVersion = Integer.parseInt(request.getParam("sourceVersion", "1"));
    contentType = request.getHeader("Content-Type");
    try {
      jsonPath = request.getParam("localIdPath") == null
        ? null : new ModuleJsonPath(request.getParam("localIdPath"));
    } catch (InvalidPathException e) {
      throw new IllegalArgumentException("malformed 'localIdPath': " + e.getMessage());
    }
    ingest = request.getParam("ingest", "true").equals("true");
    xmlFixing = request.getParam("xmlFixing", "false").equals("true");
    raw = request.getParam("raw", "false").equals("true");
    fileName = request.getParam("fileName", "<unknown>");
  }

  /**
   * Validate non null sourceId in the request.
   * @param request request
   * @return sourceId
   */
  public static String validateSourceId(HttpServerRequest request) {
    String sourceIdParam = request.getParam("sourceId");
    if (sourceIdParam == null) {
      throw new IllegalArgumentException("sourceId is a required parameter");
    }
    return sourceIdParam;

  }

}
