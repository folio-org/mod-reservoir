package org.folio.shared.index.client;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.shared.index.util.XmlJsonUtil;
import org.marc4j.MarcStreamReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.converter.impl.AnselToUnicode;

@java.lang.SuppressWarnings({"squid:S106"})
public class Client {
  static final Logger log = LogManager.getLogger(Client.class);

  UUID sourceId = UUID.randomUUID();
  MultiMap headers = MultiMap.caseInsensitiveMultiMap();
  int chunkSize = 1;
  int offset;
  int currentOffset;
  int limit;
  boolean echo = false;
  Integer localSequence = 0;
  WebClient webClient;
  Vertx vertx;
  TransformerFactory transformerFactory = TransformerFactory.newInstance();
  List<Transformer> transformers = new LinkedList<>();

  /**
   * Construct client.
   * @param webClient WebClient to use
   */
  public Client(Vertx vertx, WebClient webClient) {
    headers.set(XOkapiHeaders.URL, System.getenv("OKAPI_URL"));
    headers.set(XOkapiHeaders.TOKEN, System.getenv("OKAPI_TOKEN"));
    headers.set(XOkapiHeaders.TENANT, System.getenv("OKAPI_TENANT"));
    this.webClient = webClient;
    this.vertx = vertx;
  }

  Client(Vertx vertx, WebClient webClient, String url, String token, String tenant) {
    this(vertx, webClient);
    headers.set(XOkapiHeaders.URL, url);
    headers.set(XOkapiHeaders.TOKEN, token);
    headers.set(XOkapiHeaders.TENANT, tenant);
  }

  public void setSourceId(UUID sourceId) {
    this.sourceId = sourceId;
  }

  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public void setEcho() {
    this.echo = true;
  }

  private void incrementSequence() {
    ++localSequence;
    if (!echo && (localSequence % 1000) == 0) {
      log.info("{}", localSequence);
    }
  }

  private boolean belowLimit() {
    return limit <= 0 || currentOffset < offset + limit;
  }
  
  private interface ReaderProxy {
    boolean hasNext() throws IOException;
    
    boolean readNext() throws IOException;
    
    String parseNext() throws IOException;
  }

  private class MarcReaderProxy implements ReaderProxy {
    private MarcStreamReader marcReader;
    private org.marc4j.marc.Record marcRecord;

    public MarcReaderProxy(MarcStreamReader reader) {
      if (reader == null) {
        throw new IllegalArgumentException("Argument reader cannot be null");
      }
      marcReader = reader;
    }

    public boolean hasNext() {
      return marcReader.hasNext();
    }

    public boolean readNext() {
      marcRecord = marcReader.next();
      return true;
    }

    public String parseNext() {
      char charCodingScheme = marcRecord.getLeader().getCharCodingScheme();
      if (charCodingScheme == ' ') {
        marcRecord.getLeader().setCharCodingScheme('a');
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      MarcXmlWriter writer = new MarcXmlWriter(out);
      if (charCodingScheme == ' ') {
        writer.setConverter(new AnselToUnicode());
      }
      writer.write(marcRecord);
      writer.close();
      return out.toString();
    }
  }

  private class XmlReaderProxy implements ReaderProxy {
    private XMLStreamReader xmlReader;
    private int xmlEvent;

    public XmlReaderProxy(XMLStreamReader reader) {
      if (reader == null) {
        throw new IllegalArgumentException("Argument reader cannot be null");
      }
      xmlReader = reader;
    }

    public boolean hasNext() throws IOException {
      try {
        return xmlReader.hasNext();
      } catch (XMLStreamException xse) {
        throw new IOException(xse);
      }
    }

    public boolean readNext() throws IOException {
      try {
        while (xmlReader.hasNext()) {
          xmlEvent = xmlReader.next();
          if (xmlEvent == XMLStreamConstants.START_ELEMENT 
              && "record".equals(xmlReader.getLocalName())) {
            return true;
          }
        }
        return false;
      } catch (XMLStreamException xse) {
        throw new IOException(xse);
      }
    }

    public String parseNext() throws IOException {
      try {
        return XmlJsonUtil.getSubDocument(xmlEvent, xmlReader);
      } catch (XMLStreamException xse) {
        throw new IOException(xse);
      }
    }
  }

  private void sendChunk(ReaderProxy reader, Promise<Void> promise) {
    JsonArray records = new JsonArray();
    try {
      while (belowLimit() && reader.hasNext() && records.size() < chunkSize) {
        if (reader.readNext()) {
          if (currentOffset++ < offset) {
            continue; //skip to offset
          }
          records.add(XmlJsonUtil.createIngestRecord(reader.parseNext(), transformers));
          incrementSequence();
        }
      }
    } catch (Exception e) {
      promise.fail(e);
      return;
    }
    if (records.isEmpty()) {
      if (!echo) {
        log.info("{}", localSequence);
        log.info("Next offset (resume): {}", currentOffset);
      }
      promise.complete();
      return;
    }
    JsonObject request = new JsonObject()
        .put("sourceId", sourceId)
        .put("records", records);

    if (echo) {
      System.out.println(request);
      vertx.runOnContext(x -> sendChunk(reader, promise));
    } else {
      webClient.putAbs(headers.get(XOkapiHeaders.URL) + "/shared-index/records")
          .putHeaders(headers)
          .expect(ResponsePredicate.SC_OK)
          .expect(ResponsePredicate.JSON)
          .sendJsonObject(request)
          .onFailure(promise::fail)
          .onSuccess(x -> sendChunk(reader, promise));
    }
  }

  /**
   * Initialize data for the tenant.
   * @return async result
   */
  public Future<Void> init() {
    JsonObject request = new JsonObject()
        .put("module_to", "mod-shared-index-1.0.0");
    return tenantOp(request);
  }

  /**
   * Initialize data for the tenant.
   * @return async result
   */
  public Future<Void> purge() {
    JsonObject request = new JsonObject()
        .put("purge", Boolean.TRUE)
        .put("module_to", "mod-shared-index-1.0.0");
    return tenantOp(request);
  }

  private Future<Void> tenantOp(JsonObject request) {
    String okapiUrl = headers.get(XOkapiHeaders.URL);
    return webClient.postAbs(okapiUrl + "/_/tenant")
        .putHeaders(headers)
        .sendJsonObject(request).compose(res -> {
          if (res.statusCode() == 204 || res.statusCode() == 200) {
            return Future.succeededFuture();
          } else if (res.statusCode() != 201) {
            throw new ClientException("For /_/tenant got status code " + res.statusCode());
          }
          String id = res.bodyAsJsonObject().getString("id");
          return webClient.getAbs(okapiUrl + "/_/tenant/" + id + "?wait=10000")
              .putHeaders(headers)
              .expect(ResponsePredicate.SC_OK)
              .expect(ResponsePredicate.JSON).send()
              .compose(res2 -> {
                if (Boolean.FALSE.equals(res2.bodyAsJsonObject().getBoolean("complete"))) {
                  throw new ClientException("Incomplete job");
                }
                String error = res2.bodyAsJsonObject().getString("error");
                if (error != null) {
                  return Future.failedFuture(error);
                }
                return Future.succeededFuture();
              })
              .compose(x ->
                webClient.deleteAbs(okapiUrl + "/_/tenant/" + id)
                    .putHeaders(headers)
                    .expect(ResponsePredicate.SC_NO_CONTENT)
                    .send().mapEmpty()
              );
        });
  }

  Future<Void> sendIso2709(InputStream stream) {
    return Future.<Void>future(p -> sendChunk(new MarcReaderProxy(new MarcStreamReader(stream)), p))
        .eventually(x -> {
          try {
            stream.close();
            return Future.succeededFuture();
          } catch (IOException e) {
            return Future.failedFuture(e);
          }
        });
  }

  Future<Void> sendMarcXml(InputStream stream) throws XMLStreamException {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    return Future.<Void>future(p -> sendChunk(new XmlReaderProxy(xmlStreamReader), p))
        .eventually(x -> {
          try {
            stream.close();
            return Future.succeededFuture();
          } catch (IOException e) {
            return Future.failedFuture(e);
          }
        });
  }

  /**
   * Send file to shared-index server.
   * @param fname filename
   * @return async result
   */
  @java.lang.SuppressWarnings({"squid:S2095"}) // Resources should be closed
  // stream.close in eventually , *not* in finally as that would premature close the stream.
  public Future<Void> sendFile(String fname) {
    try {
      InputStream stream = new FileInputStream(fname);
      if (fname.endsWith(".rec") || fname.endsWith(".marc") || fname.endsWith(".mrc")) {
        return sendIso2709(stream);
      } else if (fname.endsWith(".xml")) {
        return sendMarcXml(stream);
      } else {
        stream.close();
        return Future.failedFuture("filename '" + fname + "' must be end with"
            + " .xml (marcxml) or .rec (iso2709)");
      }
    } catch (XMLStreamException | IOException e) {
      return Future.failedFuture(e);
    }
  }

  /**
   * Add XSLT to the be used for each record.
   * @param fname filename of XSL stylesheet
   * @return async result
   */
  public Future<Void> setXslt(String fname) {
    try {
      Source xslt = new StreamSource(fname);
      transformers.add(transformerFactory.newTransformer(xslt));
      return Future.succeededFuture();
    } catch (TransformerConfigurationException e) {
      return Future.failedFuture(e);
    }
  }

  private static String getArgument(String [] args, int i) {
    if (i >= args.length) {
      throw new ClientException("Missing argument for option '" + args[i - 1] + "'");
    }
    return args[i];
  }

  /** Execute command line shared-index client.
   *
   * @param vertx Vertx. handcle
   * @param webClient web client
   * @param args command line args
   * @return async result
   */
  public static Future<Void> exec(Vertx vertx, WebClient webClient, String[] args) {
    Client client = new Client(vertx, webClient);
    return exec(client, args);
  }

  static Future<Void> exec(Client client, String[] args) {
    try {
      Future<Void> future = Future.succeededFuture();
      int i = 0;
      while (i < args.length) {
        String arg;
        if (args[i].startsWith("--")) {
          switch (args[i].substring(2)) {
            case "help":
              System.out.println("[options] [file..]");
              System.out.println(" --source sourceId   (defaults to random UUID)");
              System.out.println(" --chunk sz          (defaults to 1)");
              System.out.println(" --offset int        (defaults to 0)");
              System.out.println(" --limit int         (defaults to 0 - no limit)");
              System.out.println(" --xsl file          (xslt transform for inventory payload)");
              System.out.println(" --echo              (only output result)");
              System.out.println(" --init");
              System.out.println(" --purge");
              break;
            case "source":
              arg = getArgument(args, ++i);
              client.setSourceId(UUID.fromString(arg));
              break;
            case "chunk":
              arg = getArgument(args, ++i);
              client.setChunkSize(Integer.parseInt(arg));
              break;
            case "offset":
              arg = getArgument(args, ++i);
              client.setOffset(Integer.parseInt(arg));
              break;
            case "limit":
              arg = getArgument(args, ++i);
              client.setLimit(Integer.parseInt(arg));
              break;
            case "echo":
              client.setEcho();
              break;
            case "xsl":
              arg = getArgument(args, ++i);
              future = future.compose(x -> client.setXslt(arg));
              break;
            case "init":
              future = future.compose(x -> client.init());
              break;
            case "purge":
              future = future.compose(x -> client.purge());
              break;
            default:
              throw new ClientException("Unsupported option: '" + args[i] + "'");
          }
        } else {
          arg = args[i];
          if (!client.echo) {
            log.info("Offset {} Limit {} Chunk {}", client.offset, client.limit, client.chunkSize);
          }
          future = future.compose(x -> client.sendFile(arg));
          //break here otherwise args that follow will be ignored in the first call to sendChunk
          break;
        }
        i++;
      }
      return future;
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }
}
