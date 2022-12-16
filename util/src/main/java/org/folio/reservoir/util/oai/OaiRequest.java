package org.folio.reservoir.util.oai;

import io.vertx.core.Future;

public interface OaiRequest<T> {

  OaiRequest<T> set(String set);

  OaiRequest<T> metadataPrefix(String metadataPrefix);

  OaiRequest<T> from(String from);

  OaiRequest<T> until(String until);

  OaiRequest<T> token(String token);

  OaiRequest<T> limit(int limit);

  Future<OaiResponse<T>> listRecords();
}
