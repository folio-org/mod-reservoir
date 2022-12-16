package org.folio.reservoir.util.oai;

import io.netty.handler.codec.http.QueryStringEncoder;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.streams.ReadStream;
import java.util.HashMap;
import java.util.Map;
import org.folio.reservoir.util.XmlMetadataStreamParser;
import org.folio.reservoir.util.readstream.XmlFixer;
import org.folio.reservoir.util.readstream.XmlParser;

public class OaiHttpRequest<T> implements OaiRequest<T> {

  Map<String,String> queryParameters = new HashMap<>();

  final String url;

  final HttpClient httpClient;

  final XmlMetadataStreamParser<T> metadataParser;

  final boolean xmlFixing;

  /**
   * Construct OAI HTTP client request.
   * @param httpClient the Vert.x client to use HTTP communication
   * @param url OAI URL
   * @param metadataParser XML parser for the metadata section producing T
   * @param xmlFixing whether XmlFixer should be used
   */
  public OaiHttpRequest(HttpClient httpClient, String url,
      XmlMetadataStreamParser<T> metadataParser, boolean xmlFixing) {

    this.url = url;
    this.httpClient = httpClient;
    this.metadataParser = metadataParser;
    this.xmlFixing = xmlFixing;
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

  @java.lang.SuppressWarnings({"squid:S112"}) // Generic exceptions should never be thrown
  @Override
  public Future<OaiResponse<T>> listRecords() {
    QueryStringEncoder enc = new QueryStringEncoder(url);
    queryParameters.forEach(enc::addParam);
    enc.addParam("verb", "ListRecords");
    RequestOptions requestOptions = new RequestOptions()
        .setMethod(HttpMethod.GET)
        .setAbsoluteURI(enc.toString());
    return httpClient.request(requestOptions)
        .compose(HttpClientRequest::send)
        .map(httpResponse -> {
          if (httpResponse.statusCode() != 200) {
            throw new RuntimeException("OAI server returned status " + httpResponse.statusCode());
          }
          // TODO: check content-type
          // in the future check for JSON response here!
          ReadStream<Buffer> bufferReadStream = httpResponse;
          if (xmlFixing) {
            bufferReadStream = new XmlFixer(bufferReadStream);
          }
          return new OaiXmlResponse<>(XmlParser.newParser(bufferReadStream), metadataParser);
        });
  }
}
