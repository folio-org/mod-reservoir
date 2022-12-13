package org.folio.reservoir.util.oai;

import io.vertx.core.Future;

public interface OaiRequest {

  OaiRequest set(String set);

  OaiRequest metadataPrefix(String metadataPrefix);

  OaiRequest from(String from);

  OaiRequest until(String until);

  OaiRequest token(String token);

  OaiRequest limit(int limit);

  Future<OaiResponse> listRecords();
}
