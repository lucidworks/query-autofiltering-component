# query-autofiltering-component
A Query Autofiltering SearchComponent for Solr that can translate free-text queries into structured queries using index metadata.

# Introduction
The Query Autofiltering Component provides a method of inferring user intent by matching noun-phrases that are typically used for faceted-navigation into Solr filter or boost queries (depending on configuration settings) so that more precise user queries are met with more precise results. The algorithm uses a "longest contiguous phrase match" strategy which allows it to disambiguate queries where single terms are ambiguous but phrases are not. It will work when there is structured information in the form of String fields that are normally used for faceted navigation. It works across fields by building a map of search term to index field using the Lucene FieldCache (UninvertingReader in Solr5.x and above). This enables users to create multi-term queries that combine attributes across facet fields - as if they had searched and then navigated through several facet layers. To address the problem of exact-match only semantics of String fields, support for synonyms (including multi-term synonyms) and stemming was added. 

# Building from source

The buildware requires that Apache Ant is installed on the development machine. There are two versions of the component in this distribution, one for Solr 4.x installations and one for Solr 5.x. This is due to API changes introduced in Solr 5.0 for Lucene FieldCache access.  The buildware was tested with Solr 4.10.3 and Solr 5.1 respectively.

After downloading the source code distribution, cd to the appropriate directory (solr4.x or solr5.x) and type: ant

If all goes well, (BUILD SUCCESSFUL message) a Java archive file should be created as dist/query-autofiltering-component-1.0.jar. This jar file should be copied to [solr-home]/solr/lib in Solr 4.x and [solr-home]/server/lib in Solr 5.x


Note that in Solr4.x there is an intermittent classpath issue that may cause the test to fail with "fix your classpath to have tests-framework.jar before lucene-core.jar". If this happens, running the build again (ant clean test) should (eventually) yield a successful completion (YMMV for Solr < 4.10.3 but I will update this README for issues with older 4.x versions as they are identified). Note that this issue is not related to Query Autofiltering code, rather it is due to assertion failures in the Java ClassLoader layers - and does not occur with the Solr5.x build.

# Configuration

solrconfig.xml snippet:
<pre>
  &lt;!-- test query auto filter -->
  &lt;requestHandler name="/autofilter" class="org.apache.solr.handler.component.SearchHandler">
    &lt;lst name="defaults">
      &lt;str name="echoParams">explicit&lt;/str>
      &lt;str name="df">text</str>
    &lt;/lst>
    &lt;arr name="first-components">
      &lt;str>autofilter&lt;/str>
    &lt;/arr>
  &lt;/requestHandler>

  &lt;searchComponent name="autofilter" class="org.apache.solr.handler.component.QueryAutoFilteringComponent" >
    &lt;str name="synonyms">synonyms.txt&lt;/str>
  &lt;/searchComponent>
  
  &lt;!-- Needed for Autofiltering in SolrCloud -->
  &lt;searchComponent name="termsComp" class="org.apache.solr.handler.component.TermsComponent"/>
  
  &lt;requestHandler name="/terms" class="org.apache.solr.handler.component.SearchHandler">
      &lt;arr name="components">
          &lt;str>termsComp&lt;/str>
      &lt;/arr>
  &lt;/requestHandler>
</pre>

## Filter Query or Boost Query:
The Query Autofiltering component can be used in filter query or boost query mode. To use
boost mode by default, add a "boostFactor" configuration setting to the configuration:

<pre>
  &lt;searchComponent name="autofilter" class="org.apache.solr.handler.component.QueryAutoFilteringComponent" >
    &lt;str name="synonyms">synonyms.txt&lt;/str>
    &lt;int name="boostFactor">100&lt;/int>
  &lt;/searchComponent>
</pre>

To use autofiltering boost query mode "on demand" add an &amp;afb parameter to the query request as in &amp;afb=100

##Sample Data

To show the query autofiltering component in action, I created a sample data set for a hypothetical department store. The input data contains a number of fields, product_type, product_category, color, material, brand, style, consumer_type and so on. 

To build the demo, download Solr 5 (or Solr 4 if that is what your production app is on), put the schema.xml and solrconfig.xml in the solr/collection1/conf directory for Solr 4 or server/solr/configsets/basic_configs/conf for Solr 5 - or better yet, clone basic_configs/conf and create a new configset called query_autofilter_config_set and replace schema.xml and solrconfig.xml. 

Put the jar file generated from running "ant dist" - or simply "ant" (it will be in the dist/ folder and called query-autofiltering-component-1.0.jar) into solr-webapp/webapp/WEB-INF/lib for Solr 4 or server/solr-webapp/webapp/WEB-INF/lib for Solr 5.

Startup Solr (Solr 4: java -jar start.jar  Solr 5: ./bin/solr start), import the data file using the post tool (java -jar post.jar QueryAutofilteringData_1.xml) and start searching (localhost:8983/solr). 

Note - with Solr 5 you will need to create a new collection first, it ships with configuration sets, not with a pre-built collection (collection1) as in Solr 4.

To compare the behavior of the search engine with and without autofiltering, use the /autofilter handler for "with" and the default /select handler for "without".

Happy autofiltering!

#High Level Design 

[ basic control and data flow ] - build synonym maps (finite state transform)

query parsing steps
alternative parsings of a phrase - alternative parsing must be able to use the complete phrase - partial mappings are rejected in favor of longer matches (e.g. "red baron pizza" - partial match "red" will be rejected.  "white linen shirts"  - "white linen" has two alternate and complete matches - brand:"white linen" and (color:white and material:linen)
handling boolean terms in user query
verb/adjective/preposition mapping





