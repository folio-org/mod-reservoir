<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:marc="http://www.loc.gov/MARC21/slim"
    version="1.0">
  <xsl:output indent="yes" method="xml" version="1.0" encoding="UTF-8"/>
  <xsl:template match="marc:record">
    <record>
      <localIdentifier>
        <xsl:value-of select="marc:controlfield[@tag='001']"/>
      </localIdentifier>
    </record>
  </xsl:template>
</xsl:stylesheet>
