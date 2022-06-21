package org.folio.metastorage.util;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import javax.xml.stream.XMLStreamReader;

/**
 * Streaming parser for XML ala JsonParser.
 *
 * <p>Parser that reads from stream and emits XMLStreamReader events. Allows for reading
 * large XML structures.
 *
 * @see <a href="https://vertx.io/docs/apidocs/io/vertx/core/parsetools/JsonParser.html">JsonParser</a>
 */
public interface XmlParser extends ReadStream<XMLStreamReader>, Handler<Buffer> {
  static XmlParser newParser(ReadStream<Buffer> stream) {
    return new XmlParserImpl(stream);
  }

  void end();
}
