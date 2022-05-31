<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet
    version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:marc="http://www.loc.gov/MARC21/slim">

    <xsl:output indent="yes" method="xml" version="1.0" encoding="UTF-8"/>

    <xsl:template match="/">
        <collection>
            <xsl:apply-templates />
        </collection>
    </xsl:template>

    <!-- MARC meta data -->
    <xsl:template match="marc:record">

    <record>

        <original>
            <xsl:copy>
                <xsl:copy-of select="@*"/>
                <xsl:copy-of select="*"/>
            </xsl:copy>
        </original>

        <localIdentifier><xsl:value-of select="marc:controlfield[@tag='001']" /></localIdentifier>
    </record>

    </xsl:template>

    <xsl:template match="text()"/>

</xsl:stylesheet>
