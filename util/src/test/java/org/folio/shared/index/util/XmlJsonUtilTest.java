package org.folio.shared.index.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.junit.Assert;
import org.junit.Test;
import org.xml.sax.SAXException;

public class XmlJsonUtilTest {
  static final String MARCXML1_SAMPLE =
      "<record xmlns=\"http://www.loc.gov/MARC21/slim\">\n"
          + "  <leader>1234&lt;&gt;&quot;&apos;</leader>\n"
          + "</record>";

  static final JsonObject MARCJSON1_SAMPLE = new JsonObject().put("leader", "1234<>\"'");

  static final String MARCXML2_SAMPLE =
      "<record xmlns=\"http://www.loc.gov/MARC21/slim\">\n"
          + "  <leader>01010ccm a2200289   4500</leader>\n"
          + "  <controlfield tag=\"001\">a1</controlfield>\n"
          + "  <datafield tag=\"010\" ind1=\" \" ind2=\"&amp;\">\n"
          + "    <subfield code=\"a\">   70207870</subfield>\n"
          + "  </datafield>\n"
          + "  <datafield tag=\"245\">\n"
          + "    <subfield code=\"a\">Titlea</subfield>\n"
          + "    <subfield code=\"b\">Titleb</subfield>\n"
          + "  </datafield>\n"
          + "</record>";

  static final JsonObject MARCJSON2_SAMPLE = new JsonObject()
      .put("leader", "01010ccm a2200289   4500")
      .put("fields", new JsonArray()
          .add(new JsonObject().put("001", "a1"))
          .add(new JsonObject().put("010", new JsonObject()
                  .put("ind1", " ")
                  .put("ind2", "&")
                  .put("subfields", new JsonArray()
                      .add(new JsonObject()
                          .put("a", "   70207870"))
                  )
              )
          )
          .add(new JsonObject().put("245", new JsonObject()
                  .put("subfields", new JsonArray()
                      .add(new JsonObject()
                          .put("a", "Titlea"))
                      .add(new JsonObject()
                          .put("b", "Titleb"))
                  )
              )
          )
      );

  static final String MARCXML3_SAMPLE =
      "<record xmlns=\"http://www.loc.gov/MARC21/slim\">\n"
          + "  <controlfield tag=\"001\">a1</controlfield>\n"
          + "</record>";

  static final JsonObject MARCJSON3_SAMPLE = new JsonObject()
      .put("fields", new JsonArray()
          .add(new JsonObject().put("001", "a1"))
      );

  @Test
  public void testGetSubDocumentNamespace() throws XMLStreamException {
    String collection = "<a xmlns=\"http://foo.com\">\n<b type=\"1\"><c/></b><b xmlns=\"http://bar.com\"/></a>";
    InputStream stream = new ByteArrayInputStream(collection.getBytes());
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);

    List<String> docs = new ArrayList<>();
    while (xmlStreamReader.hasNext()) {
      int event = xmlStreamReader.next();
      if (event == XMLStreamConstants.START_ELEMENT && "b".equals(xmlStreamReader.getLocalName())) {
        docs.add(XmlJsonUtil.getSubDocument(event, xmlStreamReader));
      }
    }
    Assert.assertEquals(2, docs.size());
    Assert.assertEquals("<b xmlns=\"http://foo.com\" type=\"1\"><c></c></b>", docs.get(0));
    Assert.assertEquals("<b xmlns=\"http://bar.com\"></b>", docs.get(1));
  }

  @Test
  public void testGetSubDocumentCollection() throws XMLStreamException {
    String collection = "<collection>\n"
        + MARCXML1_SAMPLE
        + "To be <ignored/>"
        + MARCXML2_SAMPLE
        + "\n</collection>";
    InputStream stream = new ByteArrayInputStream(collection.getBytes());
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);

    List<String> docs = new ArrayList<>();
    while (xmlStreamReader.hasNext()) {
      int event = xmlStreamReader.next();
      if (event == XMLStreamConstants.START_ELEMENT && "record".equals(xmlStreamReader.getLocalName())) {
        docs.add(XmlJsonUtil.getSubDocument(event, xmlStreamReader));
      }
    }
    Assert.assertEquals(2, docs.size());
    Assert.assertEquals(MARCXML1_SAMPLE, docs.get(0));
    Assert.assertEquals(MARCXML2_SAMPLE, docs.get(1));
  }

  @Test
  public void testGetSubDocumentNoSub() throws XMLStreamException {
    String collection = "<tag>x</tag>";
    InputStream stream = new ByteArrayInputStream(collection.getBytes());
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);

    int event;
    Assert.assertTrue(xmlStreamReader.hasNext());
    xmlStreamReader.next();
    Assert.assertTrue(xmlStreamReader.hasNext());
    event = xmlStreamReader.next();
    Assert.assertNull(XmlJsonUtil.getSubDocument(event, xmlStreamReader));
  }

  @Test
  public void testGetSubDocumentDocType() throws XMLStreamException {
    String sub = "<tag>x</tag>";
    String collection = "<!DOCTYPE tag []>" + sub;
    InputStream stream = new ByteArrayInputStream(collection.getBytes());
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    int event = xmlStreamReader.next();
    Assert.assertEquals(sub, XmlJsonUtil.getSubDocument(event, xmlStreamReader));
  }

  @Test
  public void testMarc2DC() throws FileNotFoundException, XMLStreamException, TransformerException {
    InputStream stream = new FileInputStream("src/test/resources/record10.xml");
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    int no = 0;
    while (xmlStreamReader.hasNext()) {
      int event = xmlStreamReader.next();
      if (event == XMLStreamConstants.START_ELEMENT && "record".equals(xmlStreamReader.getLocalName())) {
        String doc = XmlJsonUtil.getSubDocument(event, xmlStreamReader);
        if (doc == null) {
          break;
        }
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Source xslt = new StreamSource("src/test/resources/MARC21slim2DC.xsl");
        Transformer transformer = transformerFactory.newTransformer(xslt);
        Source source = new StreamSource(new StringReader(doc));
        StreamResult result = new StreamResult(new StringWriter());
        transformer.transform(source, result);
        String s = result.getWriter().toString();
        Assert.assertTrue(s, s.contains("<dc:title>"));
        no++;
      }
    }
    Assert.assertEquals(10, no);
  }

  @Test
  public void testMarc2Inventory() throws FileNotFoundException, XMLStreamException, TransformerException {
    InputStream stream = new FileInputStream("src/test/resources/record10.xml");
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    int no = 0;
    while (xmlStreamReader.hasNext()) {
      int event = xmlStreamReader.next();
      if (event == XMLStreamConstants.START_ELEMENT && "record".equals(xmlStreamReader.getLocalName())) {
        String doc = XmlJsonUtil.getSubDocument(event, xmlStreamReader);
        if (doc == null) {
          break;
        }
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Source xslt = new StreamSource("../xsl/marc2inventory-instance.xsl");
        Transformer transformer = transformerFactory.newTransformer(xslt);
        Source source = new StreamSource(new StringReader(doc));
        StreamResult result = new StreamResult(new StringWriter());
        transformer.transform(source, result);
        String s = result.getWriter().toString();
        Assert.assertTrue(s, s.contains("<localIdentifier>"));
        no++;
      }
    }
    Assert.assertEquals(10, no);
  }

  @Test
  public void convertJsonToMarcXml1() {
    String got = XmlJsonUtil.convertJsonToMarcXml(MARCJSON1_SAMPLE);
    Assert.assertEquals(MARCXML1_SAMPLE, got);
  }

  @Test
  public void convertJsonToMarcXml2() {
    String got = XmlJsonUtil.convertJsonToMarcXml(MARCJSON2_SAMPLE);
    Assert.assertEquals(MARCXML2_SAMPLE, got);
  }

  @Test
  public void convertJsonToMarcXml3() {
    String got = XmlJsonUtil.convertJsonToMarcXml(MARCJSON3_SAMPLE);
    Assert.assertEquals(MARCXML3_SAMPLE, got);
  }

  @Test
  public void convertMarcXmlToJsonRecord1() throws ParserConfigurationException, IOException, SAXException {
    JsonObject got = XmlJsonUtil.convertMarcXmlToJson(MARCXML1_SAMPLE);
    Assert.assertEquals(MARCJSON1_SAMPLE, got);
    String collection = "<collection>" + MARCXML1_SAMPLE + "</collection>";
    got = XmlJsonUtil.convertMarcXmlToJson(collection);
    Assert.assertEquals(MARCJSON1_SAMPLE, got);
  }

  @Test
  public void convertMarcXmlToJsonRecord1ignore() throws ParserConfigurationException, IOException, SAXException {
    String marcXmlExtra =
        "<record>\n"
            + "  <leader>1234&lt;&gt;&quot;&apos;</leader>\n"
            + "  <record>abc</record>\n"
            + "</record>";

    JsonObject got = XmlJsonUtil.convertMarcXmlToJson(marcXmlExtra);
    Assert.assertEquals(MARCJSON1_SAMPLE, got);
    String collection = "<collection>" + marcXmlExtra + "</collection>";
    got = XmlJsonUtil.convertMarcXmlToJson(collection);
    Assert.assertEquals(MARCJSON1_SAMPLE, got);
  }

  @Test
  public void convertMarcXmlToJsonRecord2() throws ParserConfigurationException, IOException, SAXException {
    JsonObject got = XmlJsonUtil.convertMarcXmlToJson(MARCXML2_SAMPLE);
    Assert.assertEquals(MARCJSON2_SAMPLE, got);

    String collection = "<collection>" + MARCXML2_SAMPLE + "</collection>";
    got = XmlJsonUtil.convertMarcXmlToJson(collection);
    Assert.assertEquals(MARCJSON2_SAMPLE, got);
  }

  @Test
  public void convertMarcXmlToJsonRecord3() throws ParserConfigurationException, IOException, SAXException {
    JsonObject got = XmlJsonUtil.convertMarcXmlToJson(MARCXML3_SAMPLE);
    Assert.assertEquals(MARCJSON3_SAMPLE, got);

    String collection = "<collection>" + MARCXML3_SAMPLE + "</collection>";
    got = XmlJsonUtil.convertMarcXmlToJson(collection);
    Assert.assertEquals(MARCJSON3_SAMPLE, got);
  }

  @Test
  public void convertMarcXmlToJsonRecordMulti() {
    String collection = "<collection>" + MARCXML1_SAMPLE + MARCXML2_SAMPLE + "</collection>";
    Throwable t = Assert.assertThrows(IllegalArgumentException.class,
        () ->XmlJsonUtil.convertMarcXmlToJson(collection));
    Assert.assertEquals("can not handle multiple records", t.getMessage());
  }

  @Test
  public void convertMarcXmlToJsonRecordMissing()  {
    String record = "<foo/>";
    Throwable t = Assert.assertThrows(IllegalArgumentException.class,
        () ->XmlJsonUtil.convertMarcXmlToJson(record));
    Assert.assertEquals("No record element found", t.getMessage());

    String collection = "<collection><foo/></collection>";
    t = Assert.assertThrows(IllegalArgumentException.class,
        () ->XmlJsonUtil.convertMarcXmlToJson(collection));
    Assert.assertEquals("No record element found", t.getMessage());
  }

  @Test
  public void getXmlStreamerEventInfo() throws XMLStreamException {
    String collection = "<a>x</a>";
    InputStream stream = new ByteArrayInputStream(collection.getBytes());
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    int event = xmlStreamReader.next();
    Assert.assertEquals("START a", XmlJsonUtil.getXmlStreamerEventInfo(event, xmlStreamReader));
    event = xmlStreamReader.next();
    Assert.assertEquals("CHARACTERS 'x'", XmlJsonUtil.getXmlStreamerEventInfo(event, xmlStreamReader));
    event = xmlStreamReader.next();
    Assert.assertEquals("END a", XmlJsonUtil.getXmlStreamerEventInfo(event, xmlStreamReader));
    event = xmlStreamReader.next();
    Assert.assertEquals("8", XmlJsonUtil.getXmlStreamerEventInfo(event, xmlStreamReader));
  }

  @Test
  public void inventoryXmlToJson() throws XMLStreamException {
    Assert.assertThrows(javax.xml.stream.XMLStreamException.class,
        () -> XmlJsonUtil.inventoryXmlToJson("hello"));

    Assert.assertEquals(new JsonObject().put("a", null), XmlJsonUtil.inventoryXmlToJson("<a/>"));

    Assert.assertEquals(new JsonObject().put("a", "s"),
        XmlJsonUtil.inventoryXmlToJson("<a>s</a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonObject().put("b","s")),
        XmlJsonUtil.inventoryXmlToJson("<a><b>s</b></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonObject().put("b","s")),
        XmlJsonUtil.inventoryXmlToJson("<a> <b>s</b> </a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonObject()
            .put("b", "s")
            .put("c", "t")
        ),
        XmlJsonUtil.inventoryXmlToJson("<a> <b>s</b> <c>t</c> </a>"));

    Assert.assertEquals(new JsonObject()
            .put("a",
                new JsonObject()
                    .put("b", null)
                    .put("c", null)),
        XmlJsonUtil.inventoryXmlToJson("<a><b/> <c/> </a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray()),
        XmlJsonUtil.inventoryXmlToJson("<a><arr/></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray()),
        XmlJsonUtil.inventoryXmlToJson("<a><arr>1</arr></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray().add("1")),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><i>1</i></arr></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray().add("1")),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><arr>1</arr></arr></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray().add(new JsonArray().add("1"))),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><arr><arr><arr>1</arr></arr></arr></arr></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray().add(new JsonObject().put("x1","1").put("x2", "2"))),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><i><x1>1</x1><x2>2</x2></i></arr></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray()
            .add(new JsonObject()
                .put("t","1")
                .put("u","2")
                .put("v","3"))),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><i><t>1</t><u>2</u><v>3</v></i></arr></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray()
            .add(new JsonObject()
                .put("t","1"))
            .add(new JsonObject()
                .put("u","2"))
            .add(new JsonObject()
                .put("v","3"))),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><i><t>1</t></i><i><u>2</u></i><i><v>3</v></i></arr></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray()
            .add("1")
            .add("2")
            .add("3")),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><t>1</t><t>2</t><t>3</t></arr></a>"));


    Assert.assertEquals(new JsonObject().put("a", new JsonArray()
            .add(new JsonArray()
                .add(new JsonObject().put("b", "1"))
                .add(new JsonObject().put("c", "2")))),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><i><arr><i><b>1</b></i><i><c>2</c></i></arr></i></arr></a>"));

    Assert.assertEquals(new JsonObject()
            .put("record",
                new JsonObject()
                    .put("a", new JsonObject()
                        .put("b", "1"))
                    .put("u", "3")),
        XmlJsonUtil.inventoryXmlToJson(""
            + "<record>"
            + " <a><b>1</b></a>\n"
            + " <u>3</u>\n"
            + "</record>"
        ));

    Assert.assertEquals(new JsonObject()
            .put("record",
                new JsonObject()
                    .put("a", new JsonArray()
                        .add(new JsonObject().put("b", "1")))
                    .put("c", "2")),
        XmlJsonUtil.inventoryXmlToJson(""
            + "<record>"
            + "<a>"
            + "<arr>"
            + "<i><b>1</b></i>"
            + "</arr>"
            + "</a>"
            + "<c>2</c>"
            + "</record>"
        ));

    Assert.assertEquals(new JsonObject()
            .put("record",
                new JsonObject()
                    .put("a", new JsonArray()
                        .add(new JsonObject().put("b", "1")))
                    .put("c", "2")),
        XmlJsonUtil.inventoryXmlToJson(""
            + "<record>"
            + " <a>\n"
            + "   <arr>\n"
            + "  <i>   <b>1</b> </i>\n"
            + "   </arr>\n"
            + " </a>\n"
            + " <c>2</c>\n"
            + "</record>"
        ));

    Throwable t = Assert.assertThrows(IllegalArgumentException.class,
        () -> XmlJsonUtil.inventoryXmlToJson("<arr><a/></arr>"));
    Assert.assertEquals("xmlToJsonObject not returning JsonObject", t.getMessage());

    Assert.assertEquals(new JsonObject().put("a", new JsonObject().put("b", null)),
        XmlJsonUtil.inventoryXmlToJson("<a><original><a><b><c></c>1</b></a></original><b/></a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray()
            .add(new JsonObject().put("i", null))),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><i><i/></i> </arr> </a>"));

    Assert.assertEquals(new JsonObject().put("a", new JsonArray()
            .add(new JsonObject().put("b", null))
            .add(new JsonObject().put("c", null))
        ),
        XmlJsonUtil.inventoryXmlToJson("<a><arr><i><b/></i><original>x</original><i><c/></i></arr></a>"));
  }

  @Test
  public void testCreateIngestRecord1() {
    JsonObject marcPayload = new JsonObject()
        .put("record", new JsonObject().put("leader", "l"));
    {
      JsonObject inventoryPayload = new JsonObject();
      Throwable t = Assert.assertThrows(IllegalArgumentException.class,
          () -> XmlJsonUtil.createIngestRecord(marcPayload, inventoryPayload));
      Assert.assertEquals("inventory xml: missing record property", t.getMessage());
    }

    {
      JsonObject inventoryPayload = new JsonObject().put("record", new JsonObject());
      Throwable t = Assert.assertThrows(IllegalArgumentException.class,
          () -> XmlJsonUtil.createIngestRecord(marcPayload, inventoryPayload));
      Assert.assertEquals("inventory xml: missing record/localIdentifier string", t.getMessage());
    }

    JsonObject ingest = XmlJsonUtil.createIngestRecord(marcPayload, new JsonObject()
        .put("record", new JsonObject()
            .put("original", "2")
            .put("localIdentifier", "123")
            .put("instance", new JsonObject().put("a", "b"))));
    Assert.assertEquals(marcPayload, ingest.getJsonObject("marcPayload"));
    Assert.assertEquals("123", ingest.getString("localId"));
    Assert.assertEquals(new JsonObject()
            .put("localIdentifier", "123")
            .put("instance", new JsonObject().put("a", "b")),
        ingest.getJsonObject("inventoryPayload"));

    ingest = XmlJsonUtil.createIngestRecord(marcPayload, new JsonObject()
        .put("collection", new JsonObject()
            .put("record", new JsonObject()
                .put("localIdentifier", "123")
                .put("instance", new JsonObject().put("a", "b")))));
    Assert.assertEquals(marcPayload, ingest.getJsonObject("marcPayload"));
    Assert.assertEquals("123", ingest.getString("localId"));
    Assert.assertEquals(new JsonObject()
            .put("localIdentifier", "123")
            .put("instance", new JsonObject().put("a", "b")),
        ingest.getJsonObject("inventoryPayload"));
  }

  @Test
  public void testCreateIngestRecord10() throws IOException, XMLStreamException,
      TransformerException, ParserConfigurationException, SAXException {

    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Source instanceXslt = new StreamSource("../xsl/marc2inventory-instance.xsl");
    Source holdingsXslt = new StreamSource("../xsl/holdings-items-cst.xsl");
    Source librayCodesXstXslt = new StreamSource("../xsl/library-codes-cst.xsl");
    List<Transformer> transformers = List.of(
        transformerFactory.newTransformer(instanceXslt),
        transformerFactory.newTransformer(holdingsXslt),
        transformerFactory.newTransformer(librayCodesXstXslt)
    );

    InputStream stream = new FileInputStream("src/test/resources/record10.xml");
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    JsonArray ingestRecords = new JsonArray();

    while (xmlStreamReader.hasNext()) {
      int event = xmlStreamReader.next();
      if (event == XMLStreamConstants.START_ELEMENT && "record".equals(xmlStreamReader.getLocalName())) {
        String doc = XmlJsonUtil.getSubDocument(event, xmlStreamReader);
        if (doc == null) {
          break;
        }
        ingestRecords.add(XmlJsonUtil.createIngestRecord(doc, transformers));
      }
    }
    Assert.assertEquals(10, ingestRecords.size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals("a" + (i + 1), ingestRecords.getJsonObject(i).getString("localId"));
      JsonObject inventoryPayload =  ingestRecords.getJsonObject(i).getJsonObject("inventoryPayload");
      JsonObject instance = inventoryPayload.getJsonObject("instance");
      Assert.assertNotNull(inventoryPayload.encodePrettily(), instance);
      if (i == 0) {
        // test that <xsl:when test="@tag='020' and marc:subfield[@code='a']"> is in effect
        Assert.assertEquals(new JsonObject()
            .put("value","   70207870")
            .put("identifierTypeDeref", "LCCN"),
            instance.getJsonArray("identifiers").getJsonObject(1));
      }
      Assert.assertTrue(inventoryPayload.encodePrettily(), inventoryPayload.containsKey("holdingsRecords"));
      Assert.assertEquals("US-CSt", inventoryPayload.getString("institutionDeref"));
    }
  }

  @Test
  public void removeMarcField1() {
    JsonObject got = MARCJSON1_SAMPLE.copy();
    XmlJsonUtil.removeMarcField(got, "999");
    JsonObject exp = MARCJSON1_SAMPLE.copy();
    Assert.assertEquals(exp, got);
  }

  @Test
  public void removeMarcField2() {
    JsonObject got = MARCJSON2_SAMPLE.copy();
    XmlJsonUtil.removeMarcField(got, "300");
    JsonObject exp = MARCJSON2_SAMPLE.copy();
    Assert.assertEquals(exp, got);
  }

  @Test
  public void removeMarcField3() {
    JsonObject got = MARCJSON2_SAMPLE.copy();
    XmlJsonUtil.removeMarcField(got, "245");
    JsonObject exp = MARCJSON2_SAMPLE.copy();
    exp.getJsonArray("fields").remove(2);
    Assert.assertEquals(exp, got);
  }

  @Test
  public void createMarcDataField1() {
    JsonObject got = MARCJSON1_SAMPLE.copy();
    Assert.assertNull(XmlJsonUtil.lookupMarcDataField(got, "245", "1", "2"));
    JsonArray ar = XmlJsonUtil.createMarcDataField(got, "245", "1", "2");
    Assert.assertEquals(ar, XmlJsonUtil.lookupMarcDataField(got, "245", "1", "2"));
    JsonObject exp = MARCJSON1_SAMPLE.copy();
    exp.put("fields", new JsonArray().add(new JsonObject()
        .put("245", new JsonObject()
            .put("ind1", "1")
            .put("ind2", "2")
            .put("subfields", new JsonArray())
        )));
    Assert.assertEquals(exp, got);
  }

  @Test
  public void createMarcDataField2() {
    JsonObject got = MARCJSON2_SAMPLE.copy();
    JsonArray s200 = XmlJsonUtil.createMarcDataField(got, "200", "1", "2");
    Assert.assertEquals(s200, XmlJsonUtil.lookupMarcDataField(got, "200", "1", "2"));
    Assert.assertEquals(s200, XmlJsonUtil.lookupMarcDataField(got, "200", null, "2"));
    Assert.assertEquals(s200, XmlJsonUtil.lookupMarcDataField(got, "200", "1", null));
    Assert.assertNull(XmlJsonUtil.lookupMarcDataField(got, "200", "2", null));
    Assert.assertNull(XmlJsonUtil.lookupMarcDataField(got, "200", "1", "3"));
    Assert.assertNull(XmlJsonUtil.lookupMarcDataField(got, "201", "1", "2"));
    XmlJsonUtil.createMarcDataField(got, "999", " ", " ");
    JsonObject exp = MARCJSON2_SAMPLE.copy();
    exp.getJsonArray("fields")
        .add(2, new JsonObject()
            .put("200", new JsonObject()
                .put("ind1", "1")
                .put("ind2", "2")
                .put("subfields", new JsonArray())
            ))
        .add(new JsonObject()
            .put("999", new JsonObject()
                .put("ind1", " ")
                .put("ind2", " ")
                .put("subfields", new JsonArray())
            ));
    Assert.assertEquals(exp, got);
  }
}
