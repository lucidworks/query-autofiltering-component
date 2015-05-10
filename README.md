# query-autofiltering-component
A Query Autofiltering SearchComponent for Solr that can translate free-text queries into structured queries using index metadata.

# Introduction
The Query Autofiltering Component provides a method of inferring user intent by matching noun-phrases that are typically used for faceted-navigation into Solr filter or boost queries (depending on configuration settings) so that more precise user queries are met with more precise results. The algorithm uses a "longest contiguous phrase match" strategy which allows it to disambiguate queries where single terms are ambiguous but phrases are not. It will work when there is structured information in the form of String fields that are normally used for faceted navigation. It works across fields by building a map of search term to index field using the Lucene FieldCache (UninvertingReader in Solr5.x and above). This enables users to create multi-term queries that combine attributes across facet fields - as if they had searched and then navigated through several facet layers. To address the problem of exact-match only semantics of String fields, support for synonyms (including multi-term synonyms) and stemming was added. 

# Building from source
The buildware requires that Apache Ant is installed on the development machine. 

# Configuration

solrconfig.xml snippet:
<pre>
  &lt;!-- test query auto filter -->
  &lt;requestHandler name="/autofilter" class="org.apache.solr.handler.component.SearchHandler">
    &lt;lst name="defaults">
      &lt;str name="echoParams">explicit</str>
      &lt;str name="df">text</str>
    &lt;/lst>
    &lt;arr name="first-components">
      &lt;str>autofilter</str>
    &lt;/arr>
  &lt;/requestHandler>

  &lt;searchComponent name="autofilter" class="org.apache.solr.handler.component.QueryAutoFilteringComponent" >
    &lt;str name="synonyms">synonyms.txt</str>
  &lt;/searchComponent>
  
  &lt;!-- Needed for Autofiltering in SolrCloud -->
  &lt;searchComponent name="termsComp" class="org.apache.solr.handler.component.TermsComponent"/>
  
  &lt;requestHandler name="/terms" class="org.apache.solr.handler.component.SearchHandler">
      &lt;arr name="components">
          &lt;str>termsComp</str>
      &lt;/arr>
  &lt;/requestHandler>
</pre>

