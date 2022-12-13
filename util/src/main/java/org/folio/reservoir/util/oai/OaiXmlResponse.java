package org.folio.reservoir.util.oai;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import java.util.function.Consumer;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import org.folio.reservoir.util.XmlMetadataStreamParser;
import org.folio.reservoir.util.oai.OaiRecord;
import org.folio.reservoir.util.oai.OaiResponse;
import org.folio.reservoir.util.readstream.XmlParser;

public class OaiXmlResponse<T> implements OaiResponse<T>, Handler<XMLStreamReader> {

  long demand = Long.MAX_VALUE;

  boolean ended;

  final XmlParser stream;

  final XmlMetadataStreamParser<T> metadataParser;

  Handler<Throwable> exceptionHandler;

  Handler<Void> endHandler;

  Handler<OaiRecord<T>> recordHandler;

  String resumptionToken;

  int level;

  String elem;

  OaiRecord<T> lastRecord;

  final StringBuilder cdata = new StringBuilder();

  int metadataLevel;

  int textLevel;

  String errorText;

  String errorCode;

  Consumer<String> handleText;

  /**
   * Create OAI XML response handler.
   * @param xmlParser OAI parser result.
   * @param metadataParser SAX based metadata parser producing T.
   */
  public OaiXmlResponse(XmlParser xmlParser, XmlMetadataStreamParser<T> metadataParser) {
    this.stream = xmlParser;
    this.metadataParser = metadataParser;
    this.stream.handler(this);
  }

  @Override
  public ReadStream<OaiRecord<T>> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public OaiResponse<T> handler(Handler<OaiRecord<T>> h) {
    recordHandler = h;
    return this;
  }

  @Override
  public OaiResponse<T> pause() {
    demand = 0L;
    return this;
  }

  @Override
  public OaiResponse<T> resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public OaiResponse<T> fetch(long l) {
    demand += l;
    if (demand < 0L) {
      demand = Long.MAX_VALUE;
    }
    if (demand > 0L) {
      stream.resume();
    }
    return this;
  }

  @Override
  public OaiResponse<T> endHandler(Handler<Void> handler) {
    if (!ended) {
      endHandler = handler;
    }
    return this;
  }

  @Override
  public String resumptionToken() {
    return resumptionToken;
  }

  @Override
  public String getError() {
    return errorCode != null
        ? errorCode + ": " + errorText
        : errorText;
  }

  @Override
  public void handle(XMLStreamReader xmlStreamReader) {
    try {
      int event = xmlStreamReader.getEventType();
      if (event == XMLStreamConstants.END_DOCUMENT) {
        if (lastRecord != null && recordHandler != null) {
          recordHandler.handle(lastRecord);
        }
        ended = true;
        if (endHandler != null) {
          endHandler.handle(null);
          endHandler = null;
        }
      } else if (event == XMLStreamConstants.START_ELEMENT) {
        startElement(xmlStreamReader);
      } else if (event == XMLStreamConstants.END_ELEMENT) {
        endElement(xmlStreamReader);
      } else if (metadataLevel != 0 && level > metadataLevel) {
        metadataParser.handle(xmlStreamReader);
      } else if (event == XMLStreamConstants.CHARACTERS) {
        cdata.append(xmlStreamReader.getText());
      }
    } catch (Exception e) {
      stream.handler(null);
      if (exceptionHandler != null) {
        exceptionHandler.handle(e);
      }
    }
  }

  private void startElement(XMLStreamReader xmlStreamReader) {
    level++;
    if (metadataLevel != 0 && level > metadataLevel) {
      metadataParser.handle(xmlStreamReader);
    } else {
      elem = xmlStreamReader.getLocalName();
      if (level == 3 && ("record".equals(elem) || "header".equals(elem))) {
        if (lastRecord != null && demand > 0L) {
          if (demand != Long.MAX_VALUE) {
            --demand;
          }
          if (recordHandler != null) {
            recordHandler.handle(lastRecord);
          }
          if (demand > 0L) {
            stream.resume();
          } else {
            stream.pause();
          }
        }
        lastRecord = new OaiRecord<>();
        metadataParser.init();
      }
      if ("header".equals(elem)) {
        for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
          if ("status".equals(xmlStreamReader.getAttributeLocalName(i))
              && "deleted".equals(xmlStreamReader.getAttributeValue(i))) {
            lastRecord.setIsDeleted(true);
          }
        }
      } else if ("metadata".equals(elem)) {
        metadataLevel = level;
      } else if ("resumptionToken".equals(elem)) {
        setHandleText(text -> resumptionToken = text);
      } else if ("datestamp".equals(elem)) {
        setHandleText(text -> lastRecord.setDatestamp(text));
      } else if ("identifier".equals(elem)) {
        setHandleText(text -> lastRecord.setIdentifier(text));
      } else if ("error".equals(elem)) {
        setHandleText(text -> errorText = text);
        errorCode = xmlStreamReader.getAttributeValue(null, "code");
      }
    }
  }

  private void endElement(XMLStreamReader xmlStreamReader) {
    level--;
    if (metadataLevel != 0) {
      if (level > metadataLevel) {
        metadataParser.handle(xmlStreamReader);
      } else {
        lastRecord.setMetadata(metadataParser.result());
        metadataLevel = 0;
      }
    } else {
      checkHandleText();
    }
  }

  void setHandleText(Consumer<String> handle) {
    cdata.setLength(0);
    textLevel = level;
    handleText = handle;
  }

  void checkHandleText() {
    if (level >= textLevel) {
      return;
    }
    if (handleText != null) {
      handleText.accept(cdata.toString());
    }
    cdata.setLength(0);
    handleText = null;
  }
}
