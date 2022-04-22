package org.folio.shared.index.util;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XmlJsonUtil {
  private static final Logger LOGGER = LogManager.getLogger(XmlJsonUtil.class);
  private static final String COLLECTION_LABEL = "collection";
  private static final String RECORD_LABEL = "record";
  private static final String LEADER_LABEL = "leader";
  private static final String DATAFIELD_LABEL = "datafield";
  private static final String CONTROLFIELD_LABEL = "controlfield";
  private static final String TAG_LABEL = "tag";
  private static final String SUBFIELD_LABEL = "subfield";
  private static final String SUBFIELDS_LABEL = "subfields";
  private static final String CODE_LABEL = "code";

  private static final String FIELDS_LABEL = "fields";

  private XmlJsonUtil() { }

  /** Convert MARC-in-JSON to MARCXML.
   *
   * @param obj MARC-in-JSON object
   * @return XML with record root element
   */
  public static String convertJsonToMarcXml(JsonObject obj) {
    StringBuilder s = new StringBuilder();
    s.append("<" + RECORD_LABEL + " xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
    String leader = obj.getString(LEADER_LABEL);
    if (leader != null) {
      s.append("  <" + LEADER_LABEL + ">" + encodeXmlText(leader) + "</" + LEADER_LABEL + ">\n");
    }
    JsonArray fields = obj.getJsonArray(FIELDS_LABEL);
    if (fields !=  null) {
      for (int i = 0; i < fields.size(); i++) {
        JsonObject control = fields.getJsonObject(i);
        control.fieldNames().forEach(f -> {
          Object fieldValue = control.getValue(f);
          if (fieldValue instanceof String) {
            s.append("  <" + CONTROLFIELD_LABEL + " tag=\"");
            s.append(encodeXmlText(f));
            s.append("\">");
            s.append(encodeXmlText((String) fieldValue));
            s.append("</controlfield>\n");
          }
          if (fieldValue instanceof JsonObject) {
            JsonObject fieldObject = (JsonObject) fieldValue;
            s.append("  <datafield tag=\"");
            s.append(encodeXmlText(f));
            for (int j = 1; j <= 9; j++) { // ISO 2709 allows more than 2 indicators
              String indicatorValue = fieldObject.getString("ind" + j);
              if (indicatorValue != null) {
                s.append("\" ind" + j + "=\"");
                s.append(encodeXmlText(indicatorValue));
              }
            }
            s.append("\">\n");
            JsonArray subfields = fieldObject.getJsonArray(SUBFIELDS_LABEL);
            for (int j = 0; j < subfields.size(); j++) {
              JsonObject subfieldObject = subfields.getJsonObject(j);
              subfieldObject.fieldNames().forEach(sub -> {
                s.append("    <" + SUBFIELD_LABEL);
                s.append(" code=\"" + encodeXmlText(sub) + "\">");
                s.append(subfieldObject.getString(sub));
                s.append("</" + SUBFIELD_LABEL + ">\n");
              });
            }
            s.append("  </datafield>\n");
          }
        });
      }
    }
    s.append("</record>");
    return s.toString();
  }

  /**
   * Convert MARCXML to MARC-in-JSON.
   * @param marcXml MARCXML XML string
   * @return JSON object.
   * @throws SAXException some sax exception
   * @throws ParserConfigurationException problem with XML parser
   * @throws IOException other IO error
   */
  public static JsonObject convertMarcXmlToJson(String marcXml)
      throws SAXException, ParserConfigurationException, IOException {

    JsonObject marcJson = new JsonObject();
    JsonArray fields = new JsonArray();
    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    Document document = documentBuilder.parse(new InputSource(new StringReader(marcXml)));
    Element root = document.getDocumentElement();
    Element recordElement = null;
    if (RECORD_LABEL.equals(root.getLocalName())) {
      recordElement = root;
    } else if (COLLECTION_LABEL.equals(root.getLocalName())) {
      Node node = root.getFirstChild();
      while (node != null) {
        if (RECORD_LABEL.equals(node.getLocalName())) {
          if (recordElement != null) {
            throw new IllegalArgumentException("can not handle multiple records");
          }
          recordElement = (Element) node;
        }
        node = node.getNextSibling();
      }
    }
    if (recordElement == null) {
      throw new IllegalArgumentException("No record element found");
    }
    for (Node childNode = recordElement.getFirstChild();
         childNode != null;
         childNode = childNode.getNextSibling()) {

      if (childNode.getNodeType() != Node.ELEMENT_NODE) {
        continue;
      }
      Element childElement = (Element) childNode;
      String textContent = childElement.getTextContent();
      if (childElement.getLocalName().equals(LEADER_LABEL)) {
        marcJson.put(LEADER_LABEL, textContent);
      } else if (childElement.getLocalName().equals(CONTROLFIELD_LABEL)) {
        JsonObject field = new JsonObject();
        String marcTag = childElement.getAttribute(TAG_LABEL);
        field.put(marcTag, textContent);
        fields.add(field);
      } else if (childElement.getLocalName().equals(DATAFIELD_LABEL)) {
        JsonObject fieldContent = new JsonObject();
        if (childElement.hasAttribute("ind1")) {
          fieldContent.put("ind1", childElement.getAttribute("ind1"));
        }
        if (childElement.hasAttribute("ind1")) {
          fieldContent.put("ind2", childElement.getAttribute("ind2"));
        }
        JsonArray subfields = new JsonArray();
        fieldContent.put(SUBFIELDS_LABEL, subfields);
        NodeList nodeList = childElement.getElementsByTagNameNS("*", SUBFIELD_LABEL);
        for (int i = 0; i < nodeList.getLength(); i++) {
          Element subField = (Element) nodeList.item(i);
          String code = subField.getAttribute(CODE_LABEL);
          String content = subField.getTextContent();
          JsonObject subfieldJson = new JsonObject();
          subfieldJson.put(code, content);
          subfields.add(subfieldJson);
        }
        JsonObject field = new JsonObject();
        String marcTag = childElement.getAttribute(TAG_LABEL);
        field.put(marcTag, fieldContent);
        fields.add(field);
      }
    }
    if (!fields.isEmpty()) {
      marcJson.put(FIELDS_LABEL, fields);
    }
    return marcJson;
  }

  static String getXmlStreamerEventInfo(int event, XMLStreamReader xmlStreamReader) {
    switch (event) {
      case XMLStreamConstants.END_ELEMENT:
        return "END " + xmlStreamReader.getLocalName();
      case XMLStreamConstants.START_ELEMENT:
        return "START " + xmlStreamReader.getLocalName();
      case XMLStreamConstants.CHARACTERS:
        return "CHARACTERS '" + xmlStreamReader.getText() + "'";
      default:
        return String.valueOf(event);
    }
  }

  private static int next(XMLStreamReader xmlStreamReader) throws XMLStreamException {
    int event = xmlStreamReader.next();
    LOGGER.debug("next {}", () -> getXmlStreamerEventInfo(event, xmlStreamReader));
    return event;
  }

  static JsonArray xmlToJsonArray(int depth, XMLStreamReader xmlStreamReader, String skip)
      throws XMLStreamException {
    JsonArray ar = new JsonArray();
    while (true) {
      int event = next(xmlStreamReader);
      if (event == XMLStreamConstants.START_ELEMENT) {
        JsonObject arrayObject = new JsonObject();
        xmlToJsonObject(depth + 1, xmlStreamReader, skip, event, arrayObject);
        Iterator<String> iterator = arrayObject.fieldNames().iterator();
        // take content of "i" element
        if (iterator.hasNext()) {
          ar.add(arrayObject.getValue(iterator.next()));
        }
      } else if (event != XMLStreamConstants.CHARACTERS) {
        break;
      }
    }
    return ar;
  }

  static void xmlToJsonSkip(XMLStreamReader xmlStreamReader, int event) throws XMLStreamException {
    int level = 0;
    while (true) {
      if (event == XMLStreamConstants.END_ELEMENT) {
        level--;
        if (level == 0) {
          break;
        }
      } else if (event == XMLStreamConstants.START_ELEMENT) {
        level++;
      }
      event = next(xmlStreamReader);
    }
  }

  static Object xmlToJsonObject(int depth, XMLStreamReader xmlStreamReader, String skip, int event,
      JsonObject arrayObject) throws XMLStreamException {
    StringBuilder text = null;
    JsonObject o = arrayObject;
    JsonArray ar = null;
    while (true) {
      if (event == XMLStreamConstants.START_ELEMENT) {
        String localName = xmlStreamReader.getLocalName();
        if (arrayObject == null && "arr".equals(localName)) {
          ar = xmlToJsonArray(depth, xmlStreamReader, skip);
        } else if (skip.equals(localName)) {
          xmlToJsonSkip(xmlStreamReader, event);
        } else {
          event = next(xmlStreamReader);
          if (o == null) {
            o = new JsonObject();
          }
          o.put(localName, xmlToJsonObject(depth + 1, xmlStreamReader, skip, event, null));
          if (!xmlStreamReader.hasNext() || arrayObject != null) {
            return o;
          }
        }
      } else if (arrayObject == null && event == XMLStreamConstants.CHARACTERS) {
        if (text == null) {
          text = new StringBuilder();
        }
        text.append(xmlStreamReader.getText());
      } else {
        break;
      }
      event = next(xmlStreamReader);
    }
    if (ar != null) {
      return ar;
    } else if (o != null) {
      return o;
    } else if (text != null) {
      return text.toString();
    } else {
      return null;
    }
  }

  /**
   * Convert "inventory" XML to JSON.
   * @param xml inventory XML
   * @return json object without original record
   * @throws XMLStreamException bad XML
   */
  public static JsonObject inventoryXmlToJson(String xml) throws XMLStreamException {
    InputStream stream = new ByteArrayInputStream(xml.getBytes());
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    Object o = xmlToJsonObject(0, xmlStreamReader, "original", next(xmlStreamReader), null);
    if (o instanceof JsonObject) {
      return (JsonObject) o;
    }
    throw new IllegalArgumentException("xmlToJsonObject not returning JsonObject");
  }

  /**
   * Encode encode XML string.
   * @param s string
   * @return encoded string
   */
  public static String encodeXmlText(String s) {
    StringBuilder res = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '<':
          res.append("&lt;");
          break;
        case '>':
          res.append("&gt;");
          break;
        case '&':
          res.append("&amp;");
          break;
        case '\"':
          res.append("&quot;");
          break;
        case '\'':
          res.append("&apos;");
          break;
        default:
          res.append(c);
      }
    }
    return res.toString();
  }

  /**
   * Returns XML serialized document for node in XML.
   *
   * <p>This method does not care about namespaces. Only elements (local names), attributes
   * and text is dealt with.
   *
   * @param event event type for node that begins the subdocument
   * @param reader XML stream reader
   * @return XML document string; null if no more documents in stream
   * @throws XMLStreamException if there's an exception for the XML stream
   */
  public static String getSubDocument(int event, XMLStreamReader reader)
      throws XMLStreamException {
    int level = 0;
    Buffer buffer = Buffer.buffer();
    for (; reader.hasNext(); event = reader.next()) {
      switch (event) {
        case XMLStreamConstants.START_ELEMENT:
          level++;
          buffer.appendString("<").appendString(reader.getLocalName());
          if (level == 1) {
            String uri = reader.getNamespaceURI();
            if (uri != null) {
              buffer
                  .appendString(" xmlns=\"")
                  .appendString(uri)
                  .appendString("\"");
            }
          }
          for (int i = 0; i < reader.getAttributeCount(); i++) {
            buffer
                .appendString(" ")
                .appendString(reader.getAttributeLocalName(i))
                .appendString("=\"")
                .appendString(encodeXmlText(reader.getAttributeValue(i)))
                .appendString("\"");
          }
          buffer.appendString(">");
          break;
        case XMLStreamConstants.END_ELEMENT:
          level--;
          buffer.appendString("</").appendString(reader.getLocalName()).appendString(">");
          if (level == 0) {
            return buffer.toString();
          }
          break;
        case XMLStreamConstants.CHARACTERS:
          buffer.appendString(encodeXmlText(reader.getText()));
          break;
        default:
      }
    }
    return null;
  }

  static JsonObject createIngestRecord(JsonObject marcPayload, JsonObject stylesheetResult) {
    if (stylesheetResult.containsKey(COLLECTION_LABEL)) {
      stylesheetResult = stylesheetResult.getJsonObject(COLLECTION_LABEL);
    }
    JsonObject inventoryPayload = stylesheetResult.getJsonObject(RECORD_LABEL);
    if (inventoryPayload == null) {
      throw new IllegalArgumentException("inventory xml: missing record property");
    }
    String localId = inventoryPayload.getString("localIdentifier");
    if (localId == null) {
      throw new IllegalArgumentException("inventory xml: missing record/localIdentifier string");
    }
    inventoryPayload.remove("original");
    return new JsonObject()
        .put("localId", localId)
        .put("marcPayload", marcPayload)
        .put("inventoryPayload", inventoryPayload);
  }

  /**
   * Create ingest object with "localId", "marcPayload", "inventoryPayload".
   * @param marcXml MARC XML string
   * @param transformers List of XSLT transforms to apply
   * @return ingest JSON object
   * @throws TransformerException transformer problem
   * @throws ParserConfigurationException parser problem
   * @throws IOException input/io problem
   * @throws SAXException sax problem (Invalid XML)
   * @throws XMLStreamException xml stream problem (Invalid XML)
   */
  public static JsonObject createIngestRecord(String marcXml, List<Transformer> transformers)
      throws TransformerException, ParserConfigurationException,
      IOException, SAXException, XMLStreamException {

    String inventory = marcXml;
    for (Transformer transformer : transformers) {
      Source source = new StreamSource(new StringReader(inventory));
      StreamResult result = new StreamResult(new StringWriter());
      transformer.transform(source, result);
      inventory = result.getWriter().toString();
    }
    return createIngestRecord(XmlJsonUtil.convertMarcXmlToJson(marcXml),
        XmlJsonUtil.inventoryXmlToJson(inventory));
  }

  /**
   * Create MARC tag with indicators given.
   * @param marc MARC-in-JSON object
   * @param tag marc tag, such as "245"
   * @param ind1 indicator 1
   * @param ind2 indicator 2
   * @return fields array
   */
  public static JsonArray createMarcDataField(JsonObject marc, String tag,
      String ind1, String ind2) {

    JsonArray fields = marc.getJsonArray(FIELDS_LABEL);
    if (fields == null) {
      fields = new JsonArray();
      marc.put(FIELDS_LABEL, fields);
    }
    int i;
    for (i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      int cmp = 1;
      for (String f : field.fieldNames()) {
        cmp = tag.compareTo(f);
        if (cmp <= 0) {
          break;
        }
      }
      if (cmp <= 0) {
        break;
      }
    }
    JsonObject field = new JsonObject();
    fields.add(i, new JsonObject().put(tag, field));
    field.put("ind1", ind1);
    field.put("ind2", ind2);
    JsonArray subfields = new JsonArray();
    field.put(SUBFIELDS_LABEL, subfields);
    return subfields;
  }

  /**
   * Lookup marc field.
   * @param marc MARC-in-JSON object
   * @param tag marc tag, such as "245"
   * @param ind1 indicator1 in match ; null for any
   * @param ind2 indicator2 in match; null for any
   * @return subfields array if found; null otherwise
   */
  public static JsonArray lookupMarcDataField(JsonObject marc, String tag,
      String ind1, String ind2) {
    JsonArray fields = marc.getJsonArray(FIELDS_LABEL);
    if (fields == null) {
      return null;
    }
    for (int i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      for (String f : field.fieldNames()) {
        if (f.equals(tag)) {
          JsonObject field2 = field.getJsonObject(tag);
          if ((ind1 == null || ind1.equals(field2.getString("ind1")))
              && (ind2 == null || ind2.equals(field2.getString("ind2")))) {
            return field2.getJsonArray(SUBFIELDS_LABEL);
          }
        }
      }
    }
    return null;
  }

  /**
   * Remove tag from record.
   * @param marc MARC-in-JSON object
   * @param tag fields with this tag are removed
   */
  public static void removeMarcField(JsonObject marc, String tag) {
    JsonArray fields = marc.getJsonArray(FIELDS_LABEL);
    if (fields == null) {
      return;
    }
    int i = 0;
    while (i < fields.size()) {
      JsonObject field = fields.getJsonObject(i);
      int cmp = 1;
      for (String f : field.fieldNames()) {
        cmp = tag.compareTo(f);
        if (cmp == 0) {
          break;
        }
      }
      if (cmp == 0) {
        fields.remove(i);
      } else {
        i++;
      }
    }
  }

}
