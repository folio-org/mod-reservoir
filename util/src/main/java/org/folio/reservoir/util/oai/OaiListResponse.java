package org.folio.reservoir.util.oai;

import io.vertx.core.streams.ReadStream;

public interface OaiListResponse<T> extends ReadStream<OaiRecord<T>> {

  String getResumptionToken();

  String getError();
}
