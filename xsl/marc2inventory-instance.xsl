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

        <!-- Information needed for storing source record in union catalog context -->
        <institutionDerefHere/>
        <localIdentifier><xsl:value-of select="marc:controlfield[@tag='001']" /></localIdentifier>

        <!-- Bibliographic record for FOLIO inventory -->
        <instance>
            <source>MARC</source>

            <!-- Instance type ID (resource type) -->
            <instanceTypeDeref>
                <!-- UUIDs for resource types -->
                <xsl:choose>
                <xsl:when test="substring(marc:leader,7,1)='a'">txt</xsl:when> <!-- language material : text -->
                <xsl:when test="substring(marc:leader,7,1)='c'">ntm</xsl:when> <!-- notated music : notated music -->
                <xsl:when test="substring(marc:leader,7,1)='d'">ntm</xsl:when> <!-- manuscript notated music : notated music -> notated music -->
                <xsl:when test="substring(marc:leader,7,1)='e'">cri</xsl:when> <!-- cartographic material : cartographic image -->
                <xsl:when test="substring(marc:leader,7,1)='f'">xxx</xsl:when> <!-- other --> <!-- manuscript cartographic material : ? -->
                <xsl:when test="substring(marc:leader,7,1)='g'">sti</xsl:when> <!-- projected image : still image -->
                <xsl:when test="substring(marc:leader,7,1)='i'">snd</xsl:when> <!-- nonmusical sound recording : sounds -->
                <xsl:when test="substring(marc:leader,7,1)='j'">prm</xsl:when> <!-- musical sound recording : performed music -->
                <xsl:when test="substring(marc:leader,7,1)='k'">xxx</xsl:when> <!-- other --> <!-- two-dimensional nonprojectable graphic : ?-->
                <xsl:when test="substring(marc:leader,7,1)='m'">cod</xsl:when> <!-- computer file : computer dataset -->
                <xsl:when test="substring(marc:leader,7,1)='o'">xxx</xsl:when> <!-- kit : other -->
                <xsl:when test="substring(marc:leader,7,1)='p'">xxx</xsl:when> <!-- mixed material : other -->
                <xsl:when test="substring(marc:leader,7,1)='r'">tdf</xsl:when> <!-- three-dimensional artifact or naturally occurring object : three-dimensional form -->
                <xsl:when test="substring(marc:leader,7,1)='t'">txt</xsl:when> <!-- manuscript language material : text -->
                <xsl:otherwise>xxx</xsl:otherwise>                             <!--  : other -->
                </xsl:choose>
            </instanceTypeDeref>

            <!-- Identifiers -->
            <xsl:if test="marc:datafield[@tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='074']
                        or marc:controlfield[@tag='001']">
                <identifiers>
                <arr>
                <xsl:for-each select="marc:controlfield[@tag='001']">
                    <i>
                    <value><xsl:value-of select="."/></value>
                    <!-- A subsequent library specific transformation (style sheet)
                        must replace this tag with the actual identifierTypeId for
                        the record identifer type of the given library -->
                    <identifierTypeDerefHere/>
                    </i>
                </xsl:for-each>
                <xsl:for-each select="marc:datafield[@tag='001' or @tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='074']">
                    <i>
                    <xsl:choose>
                        <xsl:when test="@tag='010' and marc:subfield[@code='a']">
                        <value>
                            <xsl:value-of select="marc:subfield[@code='a']"/>
                        </value>
                        <identifierTypeDeref>LCCN</identifierTypeDeref> <!-- LCCN -->
                        </xsl:when>
                        <xsl:when test="@tag='020' and marc:subfield[@code='a']">
                        <value>
                            <xsl:value-of select="marc:subfield[@code='a']"/>
                        </value>
                        <identifierTypeDeref>ISBN</identifierTypeDeref> <!-- ISBN -->
                        </xsl:when>
                        <xsl:when test="@tag='022' and marc:subfield[@code='a']">
                        <value>
                            <xsl:value-of select="marc:subfield[@code='a']"/>
                        </value>
                        <identifierTypeDeref>ISSN</identifierTypeDeref> <!-- ISSN -->
                        </xsl:when>
                        <xsl:when test="@tag='024' and marc:subfield[@code='a']">
                        <value>
                            <xsl:value-of select="marc:subfield[@code='a']"/>
                        </value>
                        <identifierTypeDeref>Other standard identifier</identifierTypeDeref> <!-- Other standard identifier -->
                        </xsl:when>
                        <xsl:when test="@tag='028' and marc:subfield[@code='a']">
                        <value>
                            <xsl:value-of select="marc:subfield[@code='a']"/>
                        </value>
                        <identifierTypeDeref>Publisher or distributor number</identifierTypeDeref> <!-- Publisher number -->
                        </xsl:when>
                        <xsl:when test="@tag='035' and marc:subfield[@code='a']">
                        <value>
                            <xsl:value-of select="marc:subfield[@code='a']"/>
                        </value>
                        <identifierTypeDeref>System control number</identifierTypeDeref> <!-- System control number -->
                        </xsl:when>
                        <xsl:when test="@tag='074' and marc:subfield[@code='a']">
                        <value>
                            <xsl:value-of select="marc:subfield[@code='a']"/>
                        </value>
                        <identifierTypeDeref>GPO item number</identifierTypeDeref> <!-- GPO item number -->
                        </xsl:when>
                    </xsl:choose>
                    </i>
                </xsl:for-each>
                </arr>
                </identifiers>
            </xsl:if>

            <!-- Classifications -->
            <xsl:if test="marc:datafield[@tag='050' or @tag='060' or @tag='080' or @tag='082' or @tag='086' or @tag='090']">
                <classifications>
                <arr>
                    <xsl:for-each select="marc:datafield[@tag='050' or @tag='060' or @tag='080' or @tag='082' or @tag='086' or @tag='090']">
                    <i>
                        <xsl:choose>
                        <xsl:when test="@tag='050'">
                            <classificationNumber>
                            <xsl:for-each select="marc:subfield[@code='a' or @code='b']">
                                <xsl:if test="position() > 1">
                                <xsl:text>; </xsl:text>
                            </xsl:if>
                            <xsl:value-of select="."/>
                            </xsl:for-each>
                            </classificationNumber>
                            <classificationTypeDeref>LC</classificationTypeDeref> <!-- LC, Library of Congress -->
                        </xsl:when>
                        <xsl:when test="@tag='082'">
                            <classificationNumber>
                            <xsl:for-each select="marc:subfield[@code='a' or @code='b']">
                                <xsl:if test="position() > 1">
                                <xsl:text>; </xsl:text>
                            </xsl:if>
                            <xsl:value-of select="."/>
                            </xsl:for-each>
                            </classificationNumber>
                            <classificationTypeDeref>Dewey</classificationTypeDeref> <!-- Dewey -->
                        </xsl:when>
                        <xsl:when test="@tag='086'">
                            <classificationNumber>
                            <xsl:value-of select="marc:subfield[@code='a']"/>
                            </classificationNumber>
                            <classificationTypeDeref>SUDOC</classificationTypeDeref> <!-- SUDOC -->
                        </xsl:when>
                        </xsl:choose>
                    </i>
                    </xsl:for-each>
                </arr>
                </classifications>
            </xsl:if>

            <!-- title -->
            <title>
                <xsl:variable name="dirty-title">
                    <xsl:for-each select="marc:datafield[@tag='245'][1]/marc:subfield[@code='a' or @code='b' or @code='h' or @code='n' or @code='p']">
                        <xsl:value-of select="."/>
                        <xsl:if test="position() != last()">
                            <xsl:text> </xsl:text>
                        </xsl:if>
                    </xsl:for-each>
                </xsl:variable>
                <xsl:call-template name="remove-characters-last">
                    <xsl:with-param  name="input" select="$dirty-title" />
                    <xsl:with-param  name="characters">,-./ :;</xsl:with-param>
                </xsl:call-template>
            </title>

            <!-- Contributors -->
            <xsl:if test="marc:datafield[@tag='100' or @tag='110' or @tag='111' or @tag='700' or @tag='710' or @tag='711']">
                <contributors>
                <arr>
                    <xsl:for-each select="marc:datafield[@tag='100' or @tag='110' or @tag='111' or @tag='700' or @tag='710' or @tag='711']">
                    <i>
                        <name>
                        <xsl:for-each select="marc:subfield[@code='a' or @code='b' or @code='c' or @code='d' or @code='f' or @code='g' or @code='j' or @code='k' or @code='l' or @code='n' or @code='p' or @code='q' or @code='t' or @code='u']">
                        <xsl:if test="position() > 1">
                            <xsl:text>, </xsl:text>
                        </xsl:if>
                        <xsl:call-template name="remove-characters-last">
                            <xsl:with-param  name="input" select="." />
                            <xsl:with-param  name="characters">,-.</xsl:with-param>
                        </xsl:call-template>
                        </xsl:for-each>
                        </name>
                        <xsl:choose>
                        <xsl:when test="@tag='100' or @tag='700'">
                            <contributorNameTypeDeref>Personal name</contributorNameTypeDeref> <!-- personal name -->
                            <xsl:if test="@tag='100'">
                            <primary>true</primary>
                            </xsl:if>
                        </xsl:when>
                        <xsl:when test="@tag='110' or @tag='710'">
                            <contributorNameTypeDeref>Corporate name</contributorNameTypeDeref> <!-- corporate name -->
                        </xsl:when>
                        <xsl:when test="@tag='111' or @tage='711'">
                            <contributorNameTypeDeref>Meeting name</contributorNameTypeDeref> <!-- meeting name -->
                        </xsl:when>
                        <xsl:otherwise>
                            <contributorNameTypeDeref>Personal name</contributorNameTypeDeref> <!-- personal name -->
                        </xsl:otherwise>
                        </xsl:choose>
                        <xsl:if test="marc:subfield[@code='e' or @code='4']">
                            <contributorTypeDeref>
                                <xsl:value-of select="marc:subfield[@code='e' or @code='4']"/>
                            </contributorTypeDeref>
                        </xsl:if>
                    </i>
                    </xsl:for-each>
                </arr>
                </contributors>
            </xsl:if>

            <!-- Editions -->
            <xsl:if test="marc:datafield[@tag='250']">
                <editions>
                <arr>
                <xsl:for-each select="marc:datafield[@tag='250']">
                    <i>
                    <xsl:value-of select="marc:subfield[@code='a']"/>
                    <xsl:if test="marc:subfield[@code='b']">; <xsl:value-of select="marc:subfield[@code='b']"/></xsl:if>
                    </i>
                </xsl:for-each>
                </arr>
                </editions>
            </xsl:if>

            <!-- Publication -->
            <xsl:choose>
                <xsl:when test="marc:datafield[@tag='260' or @tag='264']">
                <publication>
                    <arr>
                    <xsl:for-each select="marc:datafield[@tag='260' or @tag='264']">
                        <i>
                        <publisher>
                            <xsl:value-of select="marc:subfield[@code='b']"/>
                        </publisher>
                        <place>
                            <xsl:value-of select="marc:subfield[@code='a']"/>
                        </place>
                        <dateOfPublication>
                            <xsl:value-of select="marc:subfield[@code='c']"/>
                        </dateOfPublication>
                        </i>
                    </xsl:for-each>
                    </arr>
                </publication>
                </xsl:when>
                <xsl:otherwise>
                <publication>
                    <arr>
                    <i>
                        <dateOfPublication>
                        <xsl:value-of select="substring(marc:controlfield[@tag='008'],8,4)"/>
                        </dateOfPublication>
                    </i>
                    </arr>
                </publication>
                </xsl:otherwise>
            </xsl:choose>

            <!-- physicalDescriptions -->
            <xsl:if test="marc:datafield[@tag='300']">
                <physicalDescriptions>
                <arr>
                    <xsl:for-each select="marc:datafield[@tag='300']">
                    <i>
                        <xsl:call-template name="remove-characters-last">
                        <xsl:with-param  name="input" select="marc:subfield[@code='a']" />
                        <xsl:with-param  name="characters">,-./ :;</xsl:with-param>
                        </xsl:call-template>
                    </i>
                    </xsl:for-each>
                </arr>
                </physicalDescriptions>
            </xsl:if>

            <!-- Subjects -->
            <xsl:if test="marc:datafield[@tag='600' or @tag='610' or @tag='611' or @tag='630' or @tag='648' or @tag='650' or @tag='651' or @tag='653' or @tag='654' or @tag='655' or @tag='656' or @tag='657' or @tag='658' or @tag='662' or @tag='69X']">
                <subjects>
                <arr>
                <xsl:for-each select="marc:datafield[@tag='600' or @tag='610' or @tag='611' or @tag='630' or @tag='648' or @tag='650' or @tag='651' or @tag='653' or @tag='654' or @tag='655' or @tag='656' or @tag='657' or @tag='658' or @tag='662' or @tag='69X']">
                    <i>
                    <xsl:for-each select="marc:subfield[@code='a' or @code='b' or @code='c' or @code='d' or @code='f' or @code='g' or @code='j' or @code='k' or @code='l' or @code='n' or @code='p' or @code='q' or @code='t' or @code='u' or @code='v' or @code='x' or @code='y' or @code='z']">
                    <xsl:if test="position() > 1">
                        <xsl:text>--</xsl:text>
                    </xsl:if>
                    <xsl:call-template name="remove-characters-last">
                        <xsl:with-param  name="input" select="." />
                        <xsl:with-param  name="characters">,-.</xsl:with-param>
                        </xsl:call-template>
                    </xsl:for-each>
                    </i>
                </xsl:for-each>
                </arr>
                </subjects>
            </xsl:if>

            <!-- Notes -->
            <xsl:if test="marc:datafield[@tag='500' or @tag='504' or @tag='505' or @tag='520']">
                <notes>
                    <arr>
                    <xsl:for-each select="marc:datafield[@tag='500' or @tag='504' or @tag='505' or @tag='520']">
                        <i>
                            <note>
                                <xsl:value-of select="normalize-space(.)"/>
                            </note>
                            <instanceNoteTypeDeref>
                                <xsl:choose>
                                    <xsl:when test='./@tag="504"'>Bibliography note</xsl:when> <!-- biliography -->
                                    <xsl:when test='./@tag="505"'>Formatted Contents Note</xsl:when> <!-- contents -->
                                    <xsl:when test='./@tag="520"'>Summary</xsl:when> <!-- summary -->
                                    <xsl:otherwise>General note</xsl:otherwise> <!-- general -->
                            </xsl:choose>
                            </instanceNoteTypeDeref>
                        </i>
                    </xsl:for-each>
                    </arr>
                </notes>
            </xsl:if>

            <!-- Additional info for creating match key in FOLIO Inventory match module -->
            <matchKey>
                <xsl:for-each select="marc:datafield[@tag='245']">
                <title>
                    <xsl:call-template name="remove-characters-last">
                    <xsl:with-param  name="input" select="marc:subfield[@code='a']" />
                    <xsl:with-param  name="characters">,-./ :;</xsl:with-param>
                    </xsl:call-template>
                </title>
                <remainder-of-title>
                <xsl:text> : </xsl:text>
                    <xsl:call-template name="remove-characters-last">
                    <xsl:with-param  name="input" select="marc:subfield[@code='b']" />
                    <xsl:with-param  name="characters">,-./ :;</xsl:with-param>
                    </xsl:call-template>
                </remainder-of-title>
                <medium>
                    <xsl:call-template name="remove-characters-last">
                    <xsl:with-param  name="input" select="marc:subfield[@code='h']" />
                    <xsl:with-param  name="characters">,-./ :;</xsl:with-param>
                    </xsl:call-template>
                </medium>
                <name-of-part-section-of-work>
                    <xsl:value-of select="marc:subfield[@code='p']" />
                </name-of-part-section-of-work>
                <number-of-part-section-of-work>
                    <xsl:value-of select="marc:subfield[@code='n']" />
                </number-of-part-section-of-work>
                <inclusive-dates>
                    <xsl:value-of select="marc:subfield[@code='f']" />
                </inclusive-dates>
                </xsl:for-each>
            </matchKey>

        </instance>

    </record>

    </xsl:template>

    <xsl:template match="text()"/>


    <xsl:template name="remove-characters-last">
        <xsl:param name="input" />
        <xsl:param name="characters"/>
        <xsl:variable name="lastcharacter" select="substring($input,string-length($input))" />
        <xsl:choose>
            <xsl:when test="$characters and $lastcharacter and contains($characters, $lastcharacter)">
            <xsl:call-template name="remove-characters-last">
                <xsl:with-param  name="input" select="substring($input,1, string-length($input)-1)" />
                <xsl:with-param  name="characters" select="$characters" />
            </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
            <xsl:value-of select="$input"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

</xsl:stylesheet>
