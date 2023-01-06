package org.folio.reservoir.util.oai;

import io.vertx.core.Future;

public interface OaiRequest<T> {

  OaiRequest<T> set(String set);

  OaiRequest<T> metadataPrefix(String metadataPrefix);

  OaiRequest<T> from(String from);

  OaiRequest<T> until(String until);

  OaiRequest<T> token(String token);

  OaiRequest<T> limit(int limit);

  OaiRequest<T> params(String k, String v);

  Future<OaiListResponse<T>> listRecords();

  // probably add listIdentifiers later

  Future<OaiRecord<T>> getRecord(String identifier);

}
