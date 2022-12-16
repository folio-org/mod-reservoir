package org.folio.reservoir.util.oai;

import io.vertx.core.streams.ReadStream;

public interface OaiResponse<T> extends ReadStream<OaiRecord<T>> {

  String resumptionToken();

  String getError();
}
