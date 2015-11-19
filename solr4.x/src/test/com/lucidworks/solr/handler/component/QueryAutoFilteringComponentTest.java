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
    System.out.println( "beforeClass( ) ..." );
    initCore("solrconfig-autofilter.xml","schema-autofilter.xml" );
  }
    
  @Override
  public void setUp() throws Exception {
      System.out.println( "setUp( )" );
    super.setUp();
  }
    
  @Override
  public void tearDown() throws Exception {
          System.out.println( "tearDown( )" );
    super.tearDown();
  }
    
  @Test
    public void foobar( ) {
        System.out.println( "FOO BAR!" );
    }
    

  @Test
  public void testColors( ) {
      System.out.println( "testColors( )..." );
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "color", "red",   "product", "shoes" ));
    assertU(adoc("id", "2", "color", "Red",   "product", "socks" ));
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
    System.out.println( "testSynonyms" );
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
    System.out.println( "testCaseInsensitive" );
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
    System.out.println( "testSynonymsCaseInsensitive" );
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
    System.out.println( "testStemming" );
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
    System.out.println( "testMinTokens" );
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
    System.out.println( "testBoostFilter" );
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
    System.out.println( "testExcludeFields" );
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
    System.out.println( "testStopWords" );
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
    System.out.println( "testRandomOrder" );
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
    System.out.println( "testBadQueries" );
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
    System.out.println( "testMultipleFieldValues" );
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
    System.out.println( "testMultipleFieldNames" );
    clearIndex();
    assertU(commit());
    //assertU(adoc("id", "1", "first_name", "Tucker", "last_name", "Thomas", "full_name", "Tucker Thomas"));
    //assertU(adoc("id", "2", "first_name", "Thomas", "last_name", "Tucker", "full_name", "Thomas Tucker"));
    assertU(adoc("id", "1", "full_name", "Tucker Thomas", "text", "Tucker Thomas"));
    assertU(adoc("id", "2", "full_name", "Thomas Tucker", "text", "Thomas Tucker"));
    assertU(commit());
      
    // should create filter query (first_name:thomas OR last_name:thomas)
    assertQ("", req(CommonParams.Q, "Thomas", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='2']" );
      
    // uses longer contiguous phrase for full_name - creates fq=full_name:"thomas tucker"
    // this breaks now because of "fix" for testAmbiguousFields
    assertQ("", req(CommonParams.Q, "Thomas Tucker", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']"
              , "//doc[./str[@name='id']='2']");
  }
    
  @Test
  public void testMultiValuedField( ) {
    System.out.println( "testMultiValuedField" );
    clearIndex();
    assertU(commit());
    assertU( multiValueDocs );
    assertU(commit());
    
    assertQ("", req(CommonParams.Q, "fast stylish", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']" );

    assertQ("", req(CommonParams.Q, "fast and stylish", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']" );
      
    assertQ("", req(CommonParams.Q, "fast or stylish", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='3']" );
  }
    
  @Test
  public void testAmbiguousFields( ) {
    System.out.println( "testAmbiguousFields" );
    clearIndex();
    assertU(commit());
    assertU( whiteAmbiguousDocs );
    assertU(commit());
     
    // should create (brand_s:"white linen" OR (color:white AND material_s:linen))
    assertQ("", req(CommonParams.Q, "white linen", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='3']" );
    
    assertQ("", req(CommonParams.Q, "white linen perfume", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']" );
      
    assertQ("", req(CommonParams.Q, "white linen shirt", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='2']" );
    
    assertQ("", req(CommonParams.Q, "mens white linen shirt", CommonParams.QT, "/autofilter" )
              , "//*[@numFound='1']" );
    
  }

    
  @Test
  public void testVerbMappings( ) {
    clearIndex();
    assertU(commit());
    assertU( musicDocs );
    assertU(commit());
        
    assertQ("", req(CommonParams.Q, "Bob Dylan Songs", CommonParams.QT, "/autofilterVRB" )
              , "//*[@numFound='3']" );
        
    assertQ("", req(CommonParams.Q, "Songs Bob Dylan wrote", CommonParams.QT, "/autofilterVRB" )
              , "//*[@numFound='2']" );
        
    assertQ("", req(CommonParams.Q, "Songs Bob Dylan performed", CommonParams.QT, "/autofilterVRB" )
              , "//*[@numFound='2']" );
        
    assertQ("", req(CommonParams.Q, "Songs Bob Dylan covered", CommonParams.QT, "/autofilterVRB" )
              , "//*[@numFound='1']" );
        
  }
    
  @Test
  public void testNounPhraseMappings( ) {
    clearIndex();
    assertU(commit());
    assertU( beatlesDocs );
    assertU(commit());
        
    assertQ("", req(CommonParams.Q, "Beatles Songs", CommonParams.QT, "/autofilterVRB" )
              , "//*[@numFound='3']" );
        
    assertQ("", req(CommonParams.Q, "Beatles Songs covered", CommonParams.QT, "/autofilterVRB" )
              , "//*[@numFound='2']" );
        
    assertQ("", req(CommonParams.Q, "Beatles Songs covered by Joan Baez", CommonParams.QT, "/autofilterVRB" )
              , "//*[@numFound='1']" );
        
    assertQ("", req(CommonParams.Q, "Songs Beatles covered", CommonParams.QT, "/autofilterVRB" )
              , "//*[@numFound='1']" );
  }
    
  private static String multiValueDocs = "<add><doc><field name=\"id\">1</field><field name=\"prop_ss\">fast</field>"
                                       + "<field name=\"prop_ss\">stylish</field></doc>"
                                       + "<doc><field name=\"id\">2</field><field name=\"prop_ss\">fast</field>"
                                       + "<field name=\"prop_ss\">powerful</field></doc>"
                                       + "<doc><field name=\"id\">3</field><field name=\"prop_ss\">stylish</field></doc></add>";
    
  private static String whiteAmbiguousDocs = "<add><doc><field name=\"id\">1</field><field name=\"product_type_s\">perfume</field>"
                                        + "<field name=\"product_category_s\">fragrences</field><field name=\"brand\">White Linen</field>"
                                        + "<field name=\"consumer_type_s\">womens</field></doc>"
                                        + "<doc><field name=\"id\">2</field><field name=\"product_type_s\">dress shirt</field>"
                                        + "<field name=\"product_category_s\">shirt</field><field name=\"color\">White</field>"
                                        + "<field name=\"material_s\">Linen</field><field name=\"consumer_type_s\">womens</field></doc>"
                                        + "<doc><field name=\"id\">3</field><field name=\"product_type_s\">dress shirt</field>"
                                        + "<field name=\"product_category_s\">shirt</field><field name=\"color\">White</field>"
                                        + "<field name=\"material_s\">Linen</field><field name=\"consumer_type_s\">mens</field></doc></add>";
    
  private static String musicDocs = "<add><doc><field name=\"id\">1</field><field name=\"title_s\">All Along the Watchtower</field>"
                                  + "<field name=\"composer_s\">Bob Dylan</field><field name=\"performer_s\">Jimi Hendrix</field>"
                                  + "<field name=\"composition_type_s\">Song</field><field name=\"version_s\">Cover</field></doc>"
                                  + "<doc><field name=\"id\">2</field><field name=\"title_s\">The Mighty Quinn</field>"
                                  + "<field name=\"composer_s\">Bob Dylan</field><field name=\"performer_s\">Bob Dylan</field>"
                                  + "<field name=\"composition_type_s\">Song</field><field name=\"version_s\">Original</field></doc>"
                                  + "<doc><field name=\"id\">3</field><field name=\"title_s\">This Land is Your Land</field>"
                                  + "<field name=\"composer_s\">Woody Guthrie</field><field name=\"performer_s\">Bob Dylan</field>"
                                  + "<field name=\"composition_type_s\">Song</field><field name=\"version_s\">Cover</field></doc></add>";
    
  private static String beatlesDocs = "<add><doc><field name=\"id\">1</field><field name=\"title_s\">Let It Be</field>"
                                    + "<field name=\"original_performer_s\">Beatles</field>"
                                    + "<field name=\"performer_s\">Joan Baez</field>"
                                    + "<field name=\"version_s\">Cover</field>"
                                    + "<field name=\"recording_type_s\">Song</field></doc>"
                                    + "<doc><field name=\"id\">2</field><field name=\"title_s\">Something</field>"
                                    + "<field name=\"original_performer_s\">Beatles</field>"
                                    + "<field name=\"performer_s\">Frank Sinatra</field>"
                                    + "<field name=\"version_s\">Cover</field>"
                                    + "<field name=\"recording_type_s\">Song</field></doc>"
                                    + "<doc><field name=\"id\">3</field><field name=\"title_s\">Honey Don't</field>"
                                    + "<field name=\"original_performer_s\">Carl Perkins</field>"
                                    + "<field name=\"performer_s\">Beatles</field>"
                                    + "<field name=\"version_s\">Cover</field>"
                                    + "<field name=\"recording_type_s\">Song</field></doc></add>";
    
}