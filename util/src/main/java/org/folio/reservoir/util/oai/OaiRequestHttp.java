package org.folio.reservoir.util.oai;

import io.netty.handler.codec.http.QueryStringEncoder;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.streams.ReadStream;
import java.util.HashMap;
import java.util.Map;
import org.folio.reservoir.util.XmlMetadataStreamParser;
import org.folio.reservoir.util.readstream.XmlFixer;
import org.folio.reservoir.util.readstream.XmlParser;

public class OaiRequestHttp<T> implements OaiRequest<T> {

  Map<String,String> queryParameters = new HashMap<>();

  final String url;

  final HttpClient httpClient;

  final XmlMetadataStreamParser<T> metadataParser;

  final MultiMap headers;

  final boolean xmlFixing;

  /**
   * Construct OAI HTTP client request.
   * @param httpClient the Vert.x client to use HTTP communication
   * @param url OAI URL
   * @param metadataParser XML parser for the metadata section producing T
   * @param xmlFixing whether XmlFixer should be used
   * @param headers HTTP headers
   */
  public OaiRequestHttp(HttpClient httpClient, String url,
      XmlMetadataStreamParser<T> metadataParser, boolean xmlFixing,
      MultiMap headers) {

    this.url = url;
    this.httpClient = httpClient;
    this.metadataParser = metadataParser;
    this.xmlFixing = xmlFixing;
    if (headers == null) {
      headers = MultiMap.caseInsensitiveMultiMap();
    }
    // perhaps later also accept application/json
    if (!headers.contains(HttpHeaders.ACCEPT)) {
      headers.add(HttpHeaders.ACCEPT, "text/xml");
    }
    this.headers = headers;
  }

  @Override
  public OaiRequest<T> set(String set) {
    queryParameters.put("set", set);
    return this;
  }

  @Override
  public OaiRequest<T> metadataPrefix(String metadataPrefix) {
    queryParameters.put("metadataPrefix", metadataPrefix);
    return this;
  }

  @Override
  public OaiRequest<T> from(String from) {
    queryParameters.put("from", from);
    return this;
  }

  @Override
  public OaiRequest<T> until(String until) {
    queryParameters.put("until", until);
    return this;
  }

  @Override
  public OaiRequest<T> token(String token) {
    queryParameters.put("resumptionToken", token);
    return this;
  }

  @Override
  public OaiRequest<T> limit(int limit) {
    queryParameters.put("limit", Integer.toString(limit));
    return this;
  }

  @Override
  public OaiRequest<T> params(String k, String v) {
    queryParameters.put(k, v);
    return this;
  }

  @Override
  public Future<OaiListResponse<T>> listRecords() {
    QueryStringEncoder enc = new QueryStringEncoder(url);
    queryParameters.forEach((n, v) -> {
      if (v != null) {
        enc.addParam(n, v);
      }
    });
    enc.addParam("verb", "ListRecords");
    RequestOptions requestOptions = new RequestOptions()
        .setMethod(HttpMethod.GET)
        .setHeaders(headers)
        .setAbsoluteURI(enc.toString());
    return httpClient.request(requestOptions)
        .compose(HttpClientRequest::send)
        .map(httpResponse -> {
          if (httpResponse.statusCode() != 200) {
            throw new OaiException("OAI server returned status " + httpResponse.statusCode());
          }
          ReadStream<Buffer> bufferReadStream = httpResponse;
          if (xmlFixing) {
            bufferReadStream = new XmlFixer(bufferReadStream);
          }
          // we may probably also handle JSON transport
          // by simply inspecting the Content-Type
          return new OaiListResponseXml<>(XmlParser.newParser(bufferReadStream), metadataParser);
        });
  }

  @Override
  public Future<OaiRecord<T>> getRecord(String identifier) {
    QueryStringEncoder enc = new QueryStringEncoder(url);
    enc.addParam("verb", "GetRecord");

    RequestOptions requestOptions = new RequestOptions()
        .setMethod(HttpMethod.GET)
        .setHeaders(headers)
        .setAbsoluteURI(enc.toString());

    return httpClient.request(requestOptions)
        .compose(HttpClientRequest::send)
        .map(httpResponse -> {
          if (httpResponse.statusCode() != 200) {
            throw new OaiException("OAI server returned status " + httpResponse.statusCode());
          }
          throw new OaiException("Not implemented");
        });
  }
}
