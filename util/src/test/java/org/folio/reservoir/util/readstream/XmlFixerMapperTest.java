package org.folio.reservoir.util.readstream;

import io.vertx.core.buffer.Buffer;
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
    XmlFixerMapper xmlFixerMapper = new XmlFixerMapper();

    xmlFixerMapper.push(Buffer.buffer("\t\r\n\f \nJerzy Borzęcki."));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll.toString(), is("\t\r\n&#xFFFD; \nJerzy Borzęcki."));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    checkEnd(xmlFixerMapper);
    assertThat(xmlFixerMapper.getNumberOfFixes(), is(1));
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

    xmlFixerMapper.push(Buffer.buffer("&#9;&#10;&#13;&#1;&#31;&#32;&#x0A;&#abc;"));
    Buffer poll = xmlFixerMapper.poll();
    assertThat(poll.toString(), is("&#9;&#10;&#13;&#xFFFD;&#xFFFD;&#32;&#x0A;&#abc;"));
    poll = xmlFixerMapper.poll();
    assertThat(poll, is(nullValue()));

    checkEnd(xmlFixerMapper);
    assertThat(xmlFixerMapper.getNumberOfFixes(), is(2));
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

}
