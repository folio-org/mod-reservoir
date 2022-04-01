<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet
  version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:marc="http://www.loc.gov/MARC21/slim"
  xmlns:oai20="http://www.openarchives.org/OAI/2.0/"
  >

  <xsl:strip-space elements="*"/>
  <xsl:output indent="yes" method="xml" version="1.0" encoding="UTF-8"/>

  <xsl:template match="collection">
    <collection>
        <xsl:apply-templates/>
    </collection>
  </xsl:template>

  <xsl:template match="record">
    <record>
        <xsl:for-each select="@* | node()">
            <xsl:copy-of select="."/>
        </xsl:for-each>
        <xsl:apply-templates/>
    </record>
  </xsl:template>

  <xsl:template match="//marc:record">
    <xsl:variable name="mt" select="substring(./marc:leader, 7, 1)"/>
    <xsl:variable name="bl" select="substring(./marc:leader, 8, 1)"/>
    <holdingsRecords>
      <arr>
      <xsl:if test="marc:datafield[@tag='999']">
        <xsl:for-each select="marc:datafield[@tag='999']">
          <xsl:sort select="./marc:subfield[@code='l']"/>
          <xsl:variable name="preloc">
            <xsl:value-of select="./preceding-sibling::marc:datafield[@tag='999'][1]/marc:subfield[@code='l']"/>
          </xsl:variable>
          <xsl:variable name="iid" select="./marc:subfield[@code='i']"/>
          <xsl:variable name="loc" select="./marc:subfield[@code='l']"/>
          <xsl:if test="not($loc=$preloc)">
            <i>
              <xsl:variable name="loc-clean" select="normalize-space($loc)"/>
              <permanentLocationDeref><xsl:value-of select="$loc-clean"/></permanentLocationDeref>
              <illPolicyDeref>
                <xsl:choose>
                  <xsl:when test="$loc-clean='xxxx'">Will lend</xsl:when>
                  <xsl:otherwise>Will not lend</xsl:otherwise>
                </xsl:choose>
              </illPolicyDeref>
              <callNumber><xsl:value-of select="./marc:subfield[@code='a']"/></callNumber>
              <callNumberTypeDeref>Library of Congress classification</callNumberTypeDeref> <!-- LC -->
              <notes>
                <arr>
                  <i>
                    <note><xsl:value-of select="concat('Location code: ', $loc-clean)"/></note>
                    <holdingsNoteTypeDeref>Note</holdingsNoteTypeDeref>
                    <staffOnly>true</staffOnly>
                  </i>
                </arr>
              </notes>
              <items>
                <arr>
                <xsl:for-each select="../marc:datafield[@tag='999']/marc:subfield[@code='l'][.=$loc]/..">
                  <i>
                    <itemIdentifier><xsl:value-of select="./marc:subfield[@code='i']"/></itemIdentifier>
                    <barcode><xsl:value-of select="./marc:subfield[@code='i']"/></barcode>
                    <copyNumber><xsl:value-of select="./marc:subfield[@code='c']"/></copyNumber>
                    <status><name>Unknown</name></status>
                    <permanentLoanTypeDeref>Can circulate</permanentLoanTypeDeref> 
                    <materialTypeDeref>
                      <!-- Mappings to ReShare specific material types, taken from OCLC table "Type of Record" -->
                      <!-- Mapping from leader position 6 -->
                      <xsl:choose>
                      <xsl:when test="$bl='s' or $bl='b'">CNR - Continuing Resources</xsl:when> <!-- CNR -->
                      <xsl:when test="$mt='a' or $mt='t'">BKS - Books</xsl:when> <!-- BKS -->
                      <xsl:when test="$mt='m'">COM - Computer Files</xsl:when>            <!-- COM -->
                      <xsl:when test="$mt='e' or $mt='f'">MAP - Maps</xsl:when> <!-- MAP -->
                      <xsl:when test="$mt='c' or $mt='d'">SCO - Scores</xsl:when> <!-- SCO -->
                      <xsl:when test="$mt='i' or $mt='j'">REC - Sound Recordings</xsl:when> <!-- REC -->
                      <xsl:when test="$mt='g'">VIS - Visual Materials</xsl:when>            <!-- VIS -->
                      <xsl:when test="$mt='p'">MIX - Mixed Materials</xsl:when>            <!-- MIX -->
                      <xsl:otherwise>Unmapped</xsl:otherwise>            <!-- Unmapped -->
                      </xsl:choose>
                    </materialTypeDeref>
                  </i>
                </xsl:for-each>
                </arr>
              </items>
            </i>
          </xsl:if>
        </xsl:for-each>
      </xsl:if>
      <xsl:if test="not(marc:datafield[@tag='999'])">
        <i>
          <permanentLocationDeref>Unmapped</permanentLocationDeref>
        </i>
      </xsl:if>
      </arr> 
     </holdingsRecords>
  </xsl:template>
  <xsl:template match="text()"/>
</xsl:stylesheet>
