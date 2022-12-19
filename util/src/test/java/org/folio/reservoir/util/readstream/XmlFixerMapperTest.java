package org.folio.reservoir.util.readstream;

import io.vertx.core.buffer.Buffer;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

// ignore warnings about Parameterized test
@SuppressWarnings({"java:S5976"})
public class XmlFixerMapperTest {

  @Test
  public void testEmpty() {
    XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();

    xmlFixerMapper.push(Buffer.buffer(""));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll.length(), is(0));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    xmlFixerMapper.end();
    poll = xmlFixerMapper.poll();
    assertThat(poll.length(), is(0));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));
  }

  public void checkEnd(XmlFixerMapper xmlFixerMapper) {
    xmlFixerMapper.end();
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll.length(), is(0));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));
  }

  @Test
  public void testGood() {
    XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();

    xmlFixerMapper.push(Buffer.buffer("<good/>"));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll.length(), is(7));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    checkEnd(xmlFixerMapper);
  }

  @Test
  public void testSingleCharBad() {
    fixerTest(Buffer.buffer("\t\r\n\f \nJerzy Borzęcki."),
        "\t\r\n&#xFFFD; \nJerzy Borzęcki.");
  }

  @Test
  public void testCharEntities1() {
    XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();

    xmlFixerMapper.push(Buffer.buffer("<x>&amp;</x>"));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll.toString(), is("<x>&amp;</x>"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    checkEnd(xmlFixerMapper);
    assertThat(xmlFixerMapper.getNumberOfFixes(), is(0));
  }

  @Test
  public void testCharEntities2() {
    XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();

    xmlFixerMapper.push(Buffer.buffer("&#9;&#10;&#13;&#1;&#31;&#32;&#x0A;&#amp;"));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll.toString(), is("&#9;&#10;&#13;&#xFFFD;&#xFFFD;&#32;&#x0A;&#xFFFD;"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    checkEnd(xmlFixerMapper);
    assertThat(xmlFixerMapper.getNumberOfFixes(), is(3));
  }

  @Test
  public void testCharEntities3() {
    XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();

    xmlFixerMapper.push(Buffer.buffer("ab&"));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));
    xmlFixerMapper.push(Buffer.buffer("#"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    xmlFixerMapper.push(Buffer.buffer("9"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    xmlFixerMapper.push(Buffer.buffer(";"));
    poll = xmlFixerMapper.poll();
    assertThat(poll.toString(), is("ab&#9;"));

    checkEnd(xmlFixerMapper);
    assertThat(xmlFixerMapper.getNumberOfFixes(), is(0));
  }

  @Test
  public void testCharEntities4() {
    XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();

    xmlFixerMapper.push(Buffer.buffer("ab&"));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));
    xmlFixerMapper.push(Buffer.buffer("#"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    xmlFixerMapper.push(Buffer.buffer("3"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    xmlFixerMapper.push(Buffer.buffer("1"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    xmlFixerMapper.push(Buffer.buffer(";"));
    poll = xmlFixerMapper.poll();
    assertThat(poll.toString(), is("ab&#xFFFD;"));

    checkEnd(xmlFixerMapper);
    assertThat(xmlFixerMapper.getNumberOfFixes(), is(1));
  }

  @Test
  public void testCharEntities5() {
    XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();

    xmlFixerMapper.push(Buffer.buffer("ab&"));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));
    xmlFixerMapper.push(Buffer.buffer("#"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    xmlFixerMapper.push(Buffer.buffer("3"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    xmlFixerMapper.end();
    poll = xmlFixerMapper.poll();
    assertThat(poll.length(), is(5));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));
    assertThat(xmlFixerMapper.getNumberOfFixes(), is(0));
  }

  @Test
  public void testCharEntities6() {
    XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();

    xmlFixerMapper.push(Buffer.buffer("ab&"));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    xmlFixerMapper.end();
    poll = xmlFixerMapper.poll();
    assertThat(poll.length(), is(3));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));
    assertThat(xmlFixerMapper.getNumberOfFixes(), is(0));
  }

  static void fixerTest(Buffer input, String expect) {
    fixerTest(input, Buffer.buffer(expect));
  }

  static void fixerTest(Buffer input, Buffer expect) {
    // pass to xmlFixerMapper in two passes + end
    // this is to further test the handling of incomplete input.
    for (int i = 0; i < input.length(); i++) {
      Buffer got = Buffer.buffer();
      Buffer b1;
      Buffer res;
      XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();
      if (i > 0) {
        // first push
        b1 = Buffer.buffer(input.getBytes(0, i));
        xmlFixerMapper.push(b1);
        while ((res = xmlFixerMapper.poll()) != null) {
          got.appendBuffer(res);
        }
      }
      // second push or only push if i == 0
      b1 = Buffer.buffer(input.getBytes(i, input.length()));
      xmlFixerMapper.push(b1);
      while ((res = xmlFixerMapper.poll()) != null) {
        got.appendBuffer(res);
      }
      xmlFixerMapper.end();
      while ((res = xmlFixerMapper.poll()) != null) {
        got.appendBuffer(res);
      }
      assertThat(got, is(expect));
    }
  }

  static Buffer createBuffer(int ... values) {
    byte [] bytes = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      bytes[i] = (byte) values[i];
    }
    return Buffer.buffer(bytes);
  }


  @Test
  public void testOk() {
    fixerTest(Buffer.buffer("a"), "a");
    fixerTest(Buffer.buffer("ab"), "ab");
    fixerTest(Buffer.buffer("abc"), "abc");
    fixerTest(Buffer.buffer("æøå"), "æøå");
  }

  private static int ARING_1 = 0xc3;
  private static int ARING_2 = 0xa5;
  private static String ARING = "\u00e5";

  private static int CJK_1 = 0xe4;
  private static int CJK_2 = 0xb8;
  private static int CJK_3 = 0x90;
  private static String CJK = "\u4e10";

  @Test
  public void testAring() {
    fixerTest(createBuffer(ARING_1, ARING_2), ARING);
  }

  @Test
  public void testCjk() {
    fixerTest(createBuffer(CJK_1, CJK_2, CJK_3), CJK);
  }

  @Test
  public void testInvalidXmlChars() {
    fixerTest(createBuffer(1), "&#xFFFD;");
    fixerTest(createBuffer(3), "&#xFFFD;");
    fixerTest(createBuffer(31), "&#xFFFD;");
    fixerTest(createBuffer(32), " ");
  }

  @Test
  public void invalidByte() {
    fixerTest(createBuffer('a', 0b11111000, 'b'), "a&#xFFFD;b");
    fixerTest(createBuffer('a', 0b11111111, 'b'), "a&#xFFFD;b");
  }

  @Test
  public void invalidMidByte() {
    fixerTest(createBuffer('a', ARING_2, 'b'), "a&#xFFFD;b");
    fixerTest(createBuffer('a', CJK_2, 'b'), "a&#xFFFD;b");
    fixerTest(createBuffer('a', ARING_2, ARING_2, 'b'), "a&#xFFFD;&#xFFFD;b");
  }

  @Test
  public void incompleteSequences() {
    fixerTest(createBuffer('a', ARING_1, 'b'), "a&#xFFFD;b");
    fixerTest(createBuffer('a', CJK_1, CJK_2, 'b'), "a&#xFFFD;b");
  }

  @Test
  public void doubleStart() {
    fixerTest(createBuffer('a', ARING_1, ARING_1, 'b'), "a&#xFFFD;&#xFFFD;b");
    fixerTest(createBuffer('a', CJK_1, CJK_1, 'b'), "a&#xFFFD;&#xFFFD;b");
  }

  @Test
  public void fixedSequence() {
    fixerTest(createBuffer('a', ARING_1, '"', '>', ARING_2, 'b'), "a" + ARING + "\">b");
    fixerTest(createBuffer('a', CJK_1, '"', '>', CJK_2, CJK_3, 'b'), "a" + CJK + "\">b");
    fixerTest(createBuffer('a', CJK_1, '0', '1', '2', CJK_2, CJK_3, 'b'), "a" + CJK + "012b");
    fixerTest(createBuffer('a', CJK_1, '\r', '\n', '\t', CJK_2, CJK_3, 'b'), "a" + CJK + "\r\n\tb");
  }

  @Test
  public void testEntitySequence() {
    fixerTest(Buffer.buffer("a&\"amp;b"), "a&amp;\"b");
    fixerTest(Buffer.buffer("a&\"apos;b"), "a&apos;\"b");
    fixerTest(Buffer.buffer("a&\"quot;b"), "a&quot;\"b");
    fixerTest(Buffer.buffer("a&\"lt;b"), "a&lt;\"b");
    fixerTest(Buffer.buffer("a&\"gt;b"), "a&gt;\"b");
    fixerTest(Buffer.buffer("a&\"bad;b"), "a&#xFFFD;\"b");
    fixerTest(Buffer.buffer("a&\">amp;b"), "a&amp;\">b");
    fixerTest(Buffer.buffer("a&\"lt;b"), "a&lt;\"b");
    fixerTest(Buffer.buffer("a&\"#33;b"), "a&#33;\"b");
    fixerTest(Buffer.buffer("a&\"#1;b"), "a&#xFFFD;\"b");
    fixerTest(Buffer.buffer("a&\"#x20;b"), "a&#x20;\"b");
    fixerTest(Buffer.buffer("a&\">#1;b"), "a&#xFFFD;\">b");
    fixerTest(Buffer.buffer("a&c b"), "a&#xFFFD;c b");
    fixerTest(Buffer.buffer("a&" + CJK), "a&#xFFFD;" + CJK);
    fixerTest(Buffer.buffer("a&>>>lt;"), "a&lt;>>>");
    // fixerTest(Buffer.buffer("a&>>>>lt;bbbbbbbbbb"), "a&lt;>>>b");
  }

  @Test
  public void tooLongToFix() {
    fixerTest(createBuffer('a', CJK_1, '0', '1', '2', '3', CJK_2, CJK_3, 'b'), "a&#xFFFD;0123&#xFFFD;&#xFFFD;b");
  }

  @Test
  public void skipFixWithXmlSubst() {
    fixerTest(createBuffer('a', CJK_1, '\n', '\f', '\t', CJK_2, CJK_3, 'b'), "a&#xFFFD;\n&#xFFFD;\t&#xFFFD;&#xFFFD;b");
    fixerTest(createBuffer('a', CJK_1, '&', '#', CJK_2, CJK_3, 'b'), "a&#xFFFD;&#xFFFD;#&#xFFFD;&#xFFFD;b");
    fixerTest(createBuffer('a', CJK_1, '&', '#', '1', ';', CJK_2, CJK_3, 'b'), "a&#xFFFD;&#xFFFD;&#xFFFD;&#xFFFD;b");
  }
}
