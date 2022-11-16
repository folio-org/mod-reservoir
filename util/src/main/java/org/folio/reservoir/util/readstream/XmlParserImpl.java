package org.folio.reservoir.util.readstream;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.streams.ReadStream;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class XmlParserImpl
    extends ReadStreamConverter<XMLStreamReader, Buffer>
    implements XmlParser {
  private final AsyncXMLStreamReader<AsyncByteArrayFeeder> parser;

  XmlParserImpl(ReadStream<Buffer> stream) {
    super(stream);
    AsyncXMLInputFactory factory = new InputFactoryImpl();
    parser = factory.createAsyncForByteArray();
  }

  @Override
  XMLStreamReader getNext(boolean ended) {
    try {
      if (parser.hasNext() && parser.next() != AsyncXMLStreamReader.EVENT_INCOMPLETE) {
        return parser;
      }
      return null;
    } catch (XMLStreamException e) {
      throw new DecodeException(e.getMessage(), e);
    }
  }

  @Override
  public void handle(Buffer buffer) {
    byte[] bytes = buffer.getBytes();
    try {
      parser.getInputFeeder().feedInput(bytes, 0, bytes.length);
    } catch (XMLStreamException e) {
      if (exceptionHandler != null) {
        exceptionHandler.handle(e);
      }
    }
    checkPending();
  }

  @Override
  public void end() {
    parser.getInputFeeder().endOfInput();
    super.end();
  }
}
