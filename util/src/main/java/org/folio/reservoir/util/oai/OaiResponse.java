package org.folio.reservoir.util.oai;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;

public interface OaiResponse extends ReadStream<OaiRecord<String>> {

  @Override
  OaiResponse handler(Handler<OaiRecord<String>> h);

  @Override
  OaiResponse endHandler(Handler<Void> h);

  String resumptionToken();
}
