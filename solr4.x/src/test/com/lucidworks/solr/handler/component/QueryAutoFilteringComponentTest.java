package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@org.apache.lucene.util.LuceneTestCase.SuppressCodecs({"Lucene3x","Lucene40"})
public class QueryAutoFilteringComponentTest  extends SolrTestCaseJ4 {
    
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-autofilter.xml","schema-autofilter.xml" );
  }
    
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
    
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
    
  @Test
  public void testColors( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "shoes" ));
    assertU(adoc("id", "2", "color", "red",   "product", "socks" ));
    assertU(adoc("id", "3", "color", "brown", "product", "socks" ));
    assertU(adoc("id", "4", "color", "green", "brand", "red lion",     "product", "socks"));
    assertU(adoc("id", "5", "color", "blue",  "brand", "green dragon", "product", "socks" ));
    assertU(adoc("id", "6", "color", "black", "brand", "buster brown", "product", "shoes" ));
    assertU(commit());
      
    assertQ("", req(CommonParams.Q, "red lion socks", CommonParams.QT, "/select" )
            , "//*[@numFound='5']"
            , "//doc[./str[@name='id']='4']"
            , "//doc[./str[@name='id']='2']"
            , "//doc[./str[@name='id']='1']"
            , "//doc[./str[@name='id']='3']"
            , "//doc[./str[@name='id']='5']");
      
    assertQ("", req(CommonParams.Q, "red socks", CommonParams.QT, "/select" )
              , "//*[@numFound='5']"
              , "//doc[./str[@name='id']='2']"
              , "//doc[./str[@name='id']='4']"
              , "//doc[./str[@name='id']='1']"
              , "//doc[./str[@name='id']='3']"
              , "//doc[./str[@name='id']='5']");

    assertQ("", req(CommonParams.Q, "red lion socks", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='4']" );
      
    assertQ("", req(CommonParams.Q, "red socks", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='2']" );

    assertQ("", req(CommonParams.Q, "brown shoes", CommonParams.QT, "/select" )
              , "//*[@numFound='3']"
              , "//doc[./str[@name='id']='1']"
              , "//doc[./str[@name='id']='3']"
              , "//doc[./str[@name='id']='6']");
      
    assertQ("", req(CommonParams.Q, "brown shoes", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='0']" );
      
  }
    
  @Test
  public void testSynonyms( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "chaise lounge" ));
    assertU(adoc("id", "2", "color", "red",   "product", "sofa" ));
    assertU(adoc("id", "3", "color", "red",   "product", "chair" ));
    assertU(commit());
      
    assertQ("", req(CommonParams.Q, "red couch", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='2']" );
      
    assertQ("", req(CommonParams.Q, "rouge sofa", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='2']" );
      
    assertQ("", req(CommonParams.Q, "red lounge chair", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='1']" );
      
    assertQ("", req(CommonParams.Q, "rouge lounge chair", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='1']" );
      
      assertQ("", req(CommonParams.Q, "crimson day bed", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='1']" );
  }
    
  @Test
  public void testCaseInsensitive( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "shoes" ));
    assertU(adoc("id", "2", "color", "red",   "product", "socks" ));
    assertU(adoc("id", "3", "color", "brown", "product", "socks" ));
    assertU(adoc("id", "4", "color", "green", "brand", "Red Lion",     "product", "socks"));
    assertU(adoc("id", "5", "color", "blue",  "brand", "Green Dragon", "product", "socks" ));
    assertU(adoc("id", "6", "color", "black", "brand", "Buster Brown", "product", "shoes" ));
    assertU(commit());
      
    assertQ("", req(CommonParams.Q, "red lion socks", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='4']" );
      
    assertQ("", req(CommonParams.Q, "Red Lion socks", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='4']" );
  }
    
  @Test
  public void testSynonymsCaseInsensitive( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "Chaise Lounge" ));
    assertU(adoc("id", "2", "color", "red",   "product", "sofa" ));
    assertU(adoc("id", "3", "color", "red",   "product", "chair" ));
    assertU(commit());
        
    assertQ("", req(CommonParams.Q, "red lounge chair", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='1']" );
        
    assertQ("", req(CommonParams.Q, "scarlet Lounge Chair", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='1']" );
      
    assertQ("", req(CommonParams.Q, "Crimson Couch", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='2']" );
        
  }

    
  @Test
  public void testStemming( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "shirt" ));
    assertU(adoc("id", "2", "color", "red",   "product", "socks" ));
    assertU(adoc("id", "3", "color", "red",   "product", "pants" ));
    assertU(adoc("id", "4", "color", "red",   "product", "sofa" ));
    assertU(commit());
      
    assertQ("", req(CommonParams.Q, "red shirts", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='1']" );
      
    assertQ("", req(CommonParams.Q, "red shirt", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='1']" );
      
    assertQ("", req(CommonParams.Q, "red couches", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='4']" );
  }
    
  @Test
  public void testMinTokens( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "shoes" ));
    assertU(adoc("id", "2", "color", "red",   "product", "socks" ));
    assertU(adoc("id", "3", "color", "green", "brand", "red lion",  "product", "socks"));
    assertU(adoc("id", "4", "brand", "red label",  "product", "whiskey"));
    assertU(commit());
      
    assertQ("", req(CommonParams.Q, "red", CommonParams.QT, "/autofilter" )
            , "//*[@numFound='2']"
            , "//doc[./str[@name='id']='1']"
            , "//doc[./str[@name='id']='2']" );

    assertQ("", req(CommonParams.Q, "red", CommonParams.QT, "/autofilter", "mt", "2" )
              , "//*[@numFound='4']" );
      
    assertQ("", req(CommonParams.Q, "red shoes", CommonParams.QT, "/autofilter", "mt", "2" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='1']" );
  }
    
  @Test
  public void testBoostFilter(  ) {
    // use autofilter handler configured with boostFactor
    clearIndex();
    assertU(commit());
    assertU(adoc( "id", "1", "color", "red",   "product", "shoes" ));
    assertU(adoc( "id", "2", "color", "red",   "product", "socks" ));
    assertU(adoc( "id", "3", "color", "brown", "product", "socks" ));
    assertU(adoc( "id", "4", "color", "green", "brand", "red lion", "product", "socks" ));
    assertU(adoc( "id", "5", "color", "blue",  "brand", "red lion", "product", "socks" ));
    assertU(adoc( "id", "6", "color", "blue",  "brand", "red dragon", "product", "socks" ));
    assertU(adoc( "id", "7", "brand", "red baron", "product", "pizza" ));
    assertU(adoc( "id", "8", "brand", "red label", "product", "whiskey" ));
    assertU(adoc( "id", "9", "brand", "red light", "product", "smoke detector" ));
    assertU(adoc( "id", "10", "brand", "red star", "product", "yeast" ));
    assertU(adoc( "id", "11", "brand", "gallo", "product", "red wine" ));
    assertU(adoc( "id", "12", "brand", "heinz", "product", "red wine vinegar" ));
    assertU(adoc( "id", "13", "brand", "dole",  "product", "red grapes" ));
    assertU(adoc( "id", "14", "brand", "acme",  "product", "red brick" ));
    assertU(commit());
      
    assertQ("", req(CommonParams.Q, "blue red dragon socks", CommonParams.QT, "/autofilterBQ", "rows", "20" )
              , "//*[@numFound='14']"
              , "//doc[./str[@name='id']='6']"
              , "//doc[./str[@name='id']='5']"
              , "//doc[./str[@name='id']='2']"
              , "//doc[./str[@name='id']='4']"
              , "//doc[./str[@name='id']='3']"
              , "//doc[./str[@name='id']='1']"
              , "//doc[./str[@name='id']='7']"
              , "//doc[./str[@name='id']='8']"
              , "//doc[./str[@name='id']='9']"
              , "//doc[./str[@name='id']='10']"
              , "//doc[./str[@name='id']='11']"
              , "//doc[./str[@name='id']='12']"
              , "//doc[./str[@name='id']='13']"
              , "//doc[./str[@name='id']='14']" );
  }
    
  @Test
  public void testExcludeFields(  ) {
    // use autofilter handler configured with excludeFields
      clearIndex();
      assertU(commit());
      assertU(adoc("id", "1", "color", "red",   "product", "shoes" ));
      assertU(adoc("id", "2", "color", "red",   "product", "socks" ));
      assertU(adoc("id", "3", "color", "green", "brand", "red lion",  "product", "socks"));
      assertU(adoc("id", "4", "brand", "red label",  "product", "whiskey"));
      assertU(commit());
      
      assertQ("", req(CommonParams.Q, "1", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='1']" );
      
      // removes 'id' as an autofilter field
      assertQ("", req(CommonParams.Q, "1", CommonParams.QT, "/autofilterEX" )
              , "//*[@numFound='0']" );
    
  }
    
  @Test
  public void testStopWords( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "shoes" ));
    assertU(adoc("id", "2", "color", "red",   "product", "socks" ));
    assertU(adoc("id", "3", "color", "green", "brand", "red lion",     "product", "socks"));
    assertU(adoc("id", "4", "color", "red",   "brand", "calvin klein", "product", "underwear"));
    assertU(adoc("id", "5", "color", "red",   "brand", "fruit of the loom", "product", "underwear"));
    assertU(commit());
      
    assertQ("", req(CommonParams.Q, "red calvin klein underwear", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='4']" );
      
    // stop words should be removed: 'by' is not part of a brand name phrase
    assertQ("", req(CommonParams.Q, "red underwear by calvin klein", CommonParams.QT, "/autofilterSW" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='4']");
      
    // stop words should not be removed from within a matching phrase
    assertQ("", req(CommonParams.Q, "red fruit of the loom underwear", CommonParams.QT, "/autofilterSW" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='5']");
  }
    
  @Test
  public void testRandomOrder( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "shoes" ));
    assertU(adoc("id", "2", "color", "red",   "product", "socks" ));
    assertU(adoc("id", "3", "color", "green", "brand", "red lion",  "product", "socks"));
    assertU(adoc("id", "4", "brand", "red label",  "product", "whiskey"));
    assertU(commit());
      
    assertQ("", req(CommonParams.Q, "red lion socks", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='3']" );
      
    assertQ("", req(CommonParams.Q, "socks red lion", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='3']" );
  }
    
  @Test
  public void testBadQueries( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "shoes" ));
    assertU(adoc("id", "2", "color", "red",   "product", "socks" ));
    assertU(adoc("id", "3", "color", "green", "brand", "red lion",  "product", "socks"));
    assertU(adoc("id", "4", "brand", "red label",  "product", "whiskey"));
    assertU(adoc("id", "5", "color", "blue",  "brand", "green dragon", "product", "socks"));
    assertU(commit());
      
    // green red tiger socks -> tiger (color:(green OR red) AND product:socks)
    assertQ("", req(CommonParams.Q, "green red tiger socks", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='2']"
              , "//doc[./str[@name='id']='2']"
              , "//doc[./str[@name='id']='3']");
     
    // green red lion socks blahblah -> blahblah (color:green AND brand:"red lion" AND product:socks)
    assertQ("", req(CommonParams.Q, "green red lion socks blahblah", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='3']" );
  }
    
  @Test
  public void testMultipleFieldValues( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "shoes" ));
    assertU(adoc("id", "2", "color", "red",   "product", "socks" ));
    assertU(adoc("id", "3", "color", "green", "brand", "red lion",  "product", "socks"));
    assertU(adoc("id", "4", "brand", "red label",  "product", "whiskey"));
    assertU(adoc("id", "5", "color", "blue",  "brand", "green dragon", "product", "socks"));
    assertU(commit());
      
    // should create filter  query: color:(red OR green) product:socks
    assertQ("", req(CommonParams.Q, "red green socks", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='2']"
              , "//doc[./str[@name='id']='2']"
              , "//doc[./str[@name='id']='3']");
  }

  @Test
  public void testMultipleFieldNames( ) {
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "first_name", "Tucker",   "last_name", "Thomas", "full_name", "Tucker Thomas"));
    assertU(adoc("id", "2", "first_name", "Thomas",   "last_name", "Tucker", "full_name", "Thomas Tucker"));
    assertU(commit());
      
    // should create filter query (first_name:thomas OR last_name_thomas)
    assertQ("", req(CommonParams.Q, "Thomas", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='2']" );
      
    assertQ("", req(CommonParams.Q, "Thomas Tucker", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='2']");
  }
}