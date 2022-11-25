package org.folio.reservoir.util.readstream;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Stream mapper based on <a href="https://github.com/FasterXML/aalto-xml">Aalto XML</a>.
 */
public class XmlMapper
    implements Mapper<Buffer, XMLStreamReader> {
  private final AsyncXMLStreamReader<AsyncByteArrayFeeder> parser;

  XmlMapper() {
    AsyncXMLInputFactory factory = new InputFactoryImpl();
    parser = factory.createAsyncForByteArray();
  }

  @Override
  public XMLStreamReader poll() {
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
  public void end() {
    parser.getInputFeeder().endOfInput();
  }

  @Override
  public void push(Buffer buffer) {
    byte[] bytes = buffer.getBytes();
    try {
      parser.getInputFeeder().feedInput(bytes, 0, bytes.length);
    } catch (XMLStreamException e) {
      throw new DecodeException(e.getMessage(), e);
    }
  }

}
