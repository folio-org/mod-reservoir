package org.folio.reservoir.util.oai.impl;

import io.netty.handler.codec.http.QueryStringEncoder;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.streams.ReadStream;
import org.folio.reservoir.util.XmlMetadataStreamParser;
import org.folio.reservoir.util.oai.OaiRequest;
import org.folio.reservoir.util.oai.OaiResponse;
import org.folio.reservoir.util.readstream.XmlFixer;
import org.folio.reservoir.util.readstream.XmlParser;

public class OaiHttpRequest<T> implements OaiRequest {

  final QueryStringEncoder queryEncoder;

  final HttpClient httpClient;

  final XmlMetadataStreamParser<T> metadataParser;

  final boolean xmlFixing;

  OaiHttpRequest(HttpClient httpClient, String url,
      XmlMetadataStreamParser<T> metadataParser, boolean xmlFixing) {

    queryEncoder = new QueryStringEncoder(url);
    this.httpClient = httpClient;
    this.metadataParser = metadataParser;
    this.xmlFixing = xmlFixing;
  }

  @Override
  public OaiRequest set(String set) {
    queryEncoder.addParam("set", set);
    return this;
  }

  @Override
  public OaiRequest metadataPrefix(String metadataPrefix) {
    queryEncoder.addParam("metadataPrefix", metadataPrefix);
    return this;
  }

  @Override
  public OaiRequest from(String from) {
    queryEncoder.addParam("from", from);
    return this;
  }

  @Override
  public OaiRequest until(String until) {
    queryEncoder.addParam("until", until);
    return this;
  }

  @Override
  public OaiRequest token(String token) {
    queryEncoder.addParam("resumptionToken", token);
    return this;
  }

  @Override
  public OaiRequest limit(int limit) {
    queryEncoder.addParam("limit", Integer.toString(limit));
    return this;
  }

  @Override
  public Future<OaiResponse> listRecords() {
    queryEncoder.addParam("verb", "ListRecords");
    RequestOptions requestOptions = new RequestOptions()
        .setMethod(HttpMethod.GET)
        .setAbsoluteURI(queryEncoder.toString());
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
