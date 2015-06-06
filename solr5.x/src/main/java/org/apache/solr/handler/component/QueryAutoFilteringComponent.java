package org.apache.solr.handler.component;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.TermsParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrEventListener;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.SolrIndexSearcher;

import org.apache.solr.client.solrj.response.TermsResponse;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Term;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.SynonymMap.Builder;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.fst.FST;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Creates filter or boost queries from freetext queries based on pattern matches with terms in stored String fields. Uses
 * the FieldCache (UninvertingIndex) to build a map of term to search field. This map is then used to parse the
 * query to detect phrases that map to specific field values. These field/value pairs can then be used to generate 
 * a filter query or a boost query if recall needs to be preserved.
 *
 * For SolrCloud, this component requires that the TermsComponent be defined in solrconfig.xml. This is used
 * to get distributed term maps.
 *
 * Compiles with Solr 5.x
 */

public class QueryAutoFilteringComponent extends QueryComponent implements SolrCoreAware, SolrEventListener {
	
  private static final Logger Log = LoggerFactory.getLogger( QueryAutoFilteringComponent.class );
    
  public static final String MINIMUM_TOKENS = "mt";
  public static final String BOOST_PARAM    = "afb";
    
  private SynonymMap fieldMap;   // Map of search terms to fieldName
  private SynonymMap synonyms;   // synonyms from synonyms.txt
  private SynonymMap termMap;    // Map of search term to indexed term
    
  private String synonymsFile;
    
  private NamedList initParams;
	
  private boolean initFieldMap = false;
    
  private String termsHandler = "/terms";
    
  private HashSet<String> excludeFields;
  private HashSet<String> stopwords;
    
  private Integer boostFactor;  // if null, use Filter Query
    
  // For multiple terms in the same field, if field is multi-valued = use AND for filter query
  private boolean useAndForMultiValuedFields = true;

  private String fieldDelim = "|";
    
  private String fieldSplitExpr = "\\|";
    
  @Override
  public void init( NamedList initArgs ) {
    List<String> excludeFields = (List<String>) initArgs.get("excludeFields");
    if (excludeFields != null) {
      this.excludeFields = new HashSet<String>( );
      for (String field : excludeFields ) {
          this.excludeFields.add( field );
      }
    }
      
    Integer boostFactor = (Integer)initArgs.get( "boostFactor" );
    if (boostFactor != null) {
      this.boostFactor = boostFactor;
    }

    String useAndForMV = (String)initArgs.get( "useAndForMultiValuedFields" );
    if (useAndForMV != null) {
      this.useAndForMultiValuedFields = useAndForMV.equalsIgnoreCase( "true" );
    }
      
    String useFieldDelim = (String)initArgs.get( "fieldDelimiter" );
    if (useFieldDelim != null) {
      this.fieldDelim = useFieldDelim;
      this.fieldSplitExpr = useFieldDelim;
    }
      
    initParams = initArgs;
  }
    
  @Override
  public void inform( SolrCore core ) {
    if (initParams != null) {
      SolrResourceLoader resourceLoader = core.getResourceLoader( );
        
      synonymsFile = (String)initParams.get( "synonyms" );
      if (synonymsFile != null) {
        Analyzer analyzer = new Analyzer() {
        @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, tokenizer );
          }
        };
                
        try {
          SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
          CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                                                                      .onUnmappableCharacter(CodingErrorAction.REPORT);
                    
          parser.parse(new InputStreamReader( resourceLoader.openResource(synonymsFile), decoder));
          this.synonyms = parser.build( );
        }
        catch ( Exception e ) {
          // ???
          Log.warn( "Parsing Synonyms Got Exception " + e );
        }
      }
        
      String stopwordsFile = (String)initParams.get( "stopwords" );
      if (stopwordsFile != null) {
        this.stopwords = new HashSet<String>( );
        try {
          BufferedReader br = new BufferedReader( new InputStreamReader( resourceLoader.openResource( stopwordsFile )));
          String line = null;
          while ((line = br.readLine( )) != null) {
            stopwords.add( line.toLowerCase( ) );
          }
          br.close( );
        }
        catch ( IOException ioe ) {
          Log.warn( "Adding Stopwords Got Exception " + ioe );
        }
      }
    }
      
    core.registerFirstSearcherListener( this );
    core.registerNewSearcherListener( this );
  }
    
  @Override
  public void postCommit() {  }

  @Override
  public void postSoftCommit() {  }
    
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    synchronized( this ) {
      initFieldMap = true;
    }
  }
    
  @Override
  public void prepare( ResponseBuilder rb ) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams( );
      
    // Only build the field map and do the processing if we are the main event
    String isShard = params.get( "isShard" );
    if (isShard != null && isShard.equals( "true" )) {
      Log.debug( "A shard query: don't process!" );
      return;
    }
      
    Log.debug( "prepare ..." );
    if (initFieldMap) {
      synchronized( this ) {
        buildFieldMap( rb );
        initFieldMap = false;
      }
    }
      
    int mintok = 1;
    String mt = params.get( MINIMUM_TOKENS );
    if ( mt != null ) {
      try {
        mintok = Integer.parseInt( mt );
      }
      catch ( NumberFormatException nfe ) {
        // ???
        mintok = 1;
      }
    }
        
    String qStr = params.get( CommonParams.Q );
    Log.debug( "query is: " + qStr );
    if (qStr.equals( "*" ) || qStr.indexOf( ":" ) > 0) {
      Log.debug( "Complex query - do not process" );
      return;
    }
      
    // tokenize the query string, if any part of it matches, remove the token from the list and
    // add a filter query with <categoryField>:value:
    StringTokenizer strtok = new StringTokenizer( qStr, " .,:;\"'" );
    ArrayList<String> queryTokens = new ArrayList<String>( );
    while (strtok.hasMoreTokens( ) ) {
      String tok = strtok.nextToken( ).toLowerCase( );
      queryTokens.add( tok );
    }
      
    if (queryTokens.size( ) >= mintok) {
      ModifiableSolrParams modParams = new ModifiableSolrParams( params );
      if (findPattern( queryTokens, rb, modParams )) {
        req.setParams( modParams );
      }
    }
  }
    
  private boolean findPattern( ArrayList<String> queryTokens, ResponseBuilder rb, ModifiableSolrParams modParams ) throws IOException {
    Log.debug( "findPattern " );

    HashSet<Integer> usedTokens = new HashSet<Integer>( );
    HashMap<String,ArrayList<String>> fieldMap = new HashMap<String,ArrayList<String>>( );
    HashMap<String,int[]> fieldPositionMap = new HashMap<String,int[]>( );
      
    String longestPhraseField = null;
    int startToken = 0;
    int lastEndToken = 0;
    while ( startToken < queryTokens.size() ) {
      Log.debug( "startToken = " + startToken );
      int endToken = startToken;
        
      while ( endToken < queryTokens.size( ) ) {
        // FieldName can be comma separated if there are more than one field name for a set of tokens
        String fieldName = getFieldNameFor( queryTokens, startToken, endToken );
        if ( fieldName != null ) {
          longestPhraseField = fieldName;
          lastEndToken = endToken;
        }
        else if ( longestPhraseField != null ) {
          break;
        }
        ++endToken;
      }
        
      if (longestPhraseField != null) {
        // create matching phrase from startToken -> endToken
        String phrase = getPhrase( queryTokens, startToken, lastEndToken );
        Log.debug( "get Indexed Term for " + phrase );
        String indexedTerm = getMappedFieldName( termMap, phrase );
        if (indexedTerm == null) {
          indexedTerm = getMappedFieldName( termMap, getStemmed( phrase ));
        }
        if (indexedTerm != null) {
          indexedTerm = indexedTerm.replace( '_', ' ' );
          if (indexedTerm.indexOf( " " ) > 0 ) {
            indexedTerm = "\"" + indexedTerm + "\"";
          }
          ArrayList<String>valList = fieldMap.get( longestPhraseField );
          if (valList == null) {
            valList = new ArrayList<String>( );
            fieldMap.put( longestPhraseField, valList );
          }
          
          Log.debug( "indexedTerm: " + indexedTerm );
          if (indexedTerm.indexOf( fieldDelim ) > 0)
          {
            String[] indexedTerms = indexedTerm.split( fieldSplitExpr );
            for (int t = 0; t < indexedTerms.length; t++) {
              valList.add( indexedTerms[t] );
            }
          }
          else {
            valList.add( indexedTerm );
          }
            
          // save startToken and lastEndToken so can use for boolean operator context
          // for multi-value fields -save the min and max of all tokens positions for the field
          int[] posArray = fieldPositionMap.get( longestPhraseField );
          if (posArray == null)
          {
            posArray = new int[2];
            posArray[0] = startToken;
            posArray[1] = lastEndToken;
            fieldPositionMap.put( longestPhraseField, posArray );
          }
          else
          {
            posArray[1] = lastEndToken;
          }
            
          longestPhraseField = null;
          for (int i = startToken; i <= lastEndToken; i++) {
            Log.debug( "adding used token at " + i );
            usedTokens.add( new Integer( i ) );
          }
          startToken = lastEndToken + 1;
        }
      }
      else {
        ++startToken;
      }
    }
      
    if (usedTokens.size( ) > 0) {
        
      String useBoost = modParams.get( BOOST_PARAM );
      Integer boostFactor = (useBoost != null) ? new Integer( useBoost ) : this.boostFactor;
      if (boostFactor == null) {
        StringBuilder qbuilder = new StringBuilder( );
        if (usedTokens.size( ) < queryTokens.size( ) ) {
          for (int i = 0; i < queryTokens.size(); i++) {
            if (boostFactor != null || usedTokens.contains( new Integer( i ) ) == false ) {
              String token = queryTokens.get( i );
              if (stopwords == null || !stopwords.contains( token.toLowerCase( ) )) {
                if (qbuilder.length() > 0) qbuilder.append( " " );
                qbuilder.append( token );
              }
            }
          }
        }
          
        Log.debug( "got qbuilder string = '" + qbuilder.toString() + "'" );
        if (qbuilder.length() == 0 && fieldMap.size() > 0) {
          // build a filter query
          Log.debug( "setting q = *:*" );
          modParams.set( "q", "*:*" );
          for (String fieldName : fieldMap.keySet() ) {
            String fq = getFilterQuery( rb, fieldName, fieldMap.get( fieldName ), fieldPositionMap.get( fieldName ), queryTokens, "" );
            Log.info( "adding filter query: " + fq );
            modParams.add( "fq", fq );
          }
        }
        else if (qbuilder.length() > 0 && fieldMap.size() > 0) {
          // build a boolean query for the fielded data, OR with remainder of query
          StringBuilder boolQ = new StringBuilder( );
          for (String fieldName : fieldMap.keySet() ) {
            if (boolQ.length() > 0) boolQ.append( " AND " );
            boolQ.append( getFilterQuery( rb, fieldName, fieldMap.get( fieldName ), fieldPositionMap.get( fieldName ), queryTokens, "" ) );
          }
          String q = qbuilder.toString( ) + " (" + boolQ.toString() + ")";
          Log.info( "setting q = '" + q + "'" );
          modParams.set( "q", q );
        }
      }
      else { // boostFactor is NOT null
        // use the original query add fielded boost clauses
        StringBuilder bbuilder = new StringBuilder( );
        String boostSuffix = "^" + boostFactor.toString( );
        bbuilder.append( getPhrase( queryTokens, 0, queryTokens.size() - 1, " " ) );
        for (String fieldName : fieldMap.keySet( ) ) {
          bbuilder.append( " " );
          bbuilder.append( getFilterQuery( rb, fieldName, fieldMap.get( fieldName ), fieldPositionMap.get( fieldName ), queryTokens, boostSuffix ) );
        }
        Log.info( "setting q = '" + bbuilder.toString()  + "'" );
        modParams.set( "q", bbuilder.toString( ) );
      }
      return true;
    }
    
    return false;
  }
    
  private String getPhrase( ArrayList<String> tokens, int startToken, int endToken ) {
    return getPhrase( tokens, startToken, endToken, "_" );
  }
    
  private String getPhrase( ArrayList<String> tokens, int startToken, int endToken, String tokenSep ) {
    StringBuilder strb = new StringBuilder( );
    for (int i = startToken; i <= endToken; i++) {
      if (i > startToken) strb.append( tokenSep );
      strb.append( tokens.get( i ) );
    }
    return strb.toString( );
  }
    
  private String getFilterQuery( ResponseBuilder rb, String fieldName, ArrayList<String> valList,
                                 int[] termPosRange, ArrayList<String> queryTokens, String suffix) {
    if (fieldName.indexOf( fieldDelim ) > 0) {
      return getFilterQuery( rb, fieldName.split( fieldSplitExpr ), valList, termPosRange, queryTokens, suffix );
    }
    if (valList.size() == 1) {
      // check if valList[0] is multi-term - if so, check if there is a single term equivalent
      // if this returns non-null, create an OR query with single term version
      // example "white linen perfume" vs "white linen shirt"  where "White Linen" is a brand
      String term = valList.get( 0 );
        
      if (term.indexOf( " " ) > 0) {
        String singleTermQuery = getSingleTermQuery( term );
        if (singleTermQuery != null) {
          StringBuilder strb = new StringBuilder( );
          strb.append( "(" ).append( fieldName ).append( ":" )
              .append( term ).append( " OR (" ).append( singleTermQuery ).append( "))" ).append( suffix );
          Log.debug( "returning composite query: " + strb.toString( ) );
          return strb.toString( );
        }
      }
        
      String query = fieldName + ":" + term + suffix;
      Log.debug( "returning single query: " + query );
      return query;
    }
    else {
      SolrIndexSearcher searcher = rb.req.getSearcher();
      IndexSchema schema = searcher.getSchema();
      SchemaField field = schema.getField(fieldName);
      boolean useAnd = field.multiValued() && useAndForMultiValuedFields;
      // if query has 'or' in it and or is at a position 'within' the values for this field ...
      if (useAnd) {
        for (int i = termPosRange[0] + 1; i < termPosRange[1]; i++ ) {
          String qToken = queryTokens.get( i );
          if (qToken.equalsIgnoreCase( "or" )) {
            useAnd = false;
            break;
          }
        }
      }
        
      StringBuilder orQ = new StringBuilder( );
      for (String val : valList ) {
        if (orQ.length() > 0) orQ.append( (useAnd ? " AND " : " OR ") );
        orQ.append( val );
      }
      return fieldName + ":(" + orQ.toString() + ")" + suffix;
    }
  }
    
  private String getFilterQuery( ResponseBuilder rb, String[] fieldNames, ArrayList<String> valList,
                                 int[] termPosRange, ArrayList<String> queryTokens, String suffix) {
    StringBuilder filterQBuilder = new StringBuilder( );
    for (int i = 0; i < fieldNames.length; i++) {
      if (i > 0) filterQBuilder.append( " OR " );
      filterQBuilder.append( getFilterQuery( rb, fieldNames[i], valList, termPosRange, queryTokens, suffix ) );
    }
    return "(" + filterQBuilder.toString() + ")";
  }
    
  private String getFieldNameFor( ArrayList<String> queryTokens, int startToken, int endToken ) throws IOException {
    String phrase = getPhrase( queryTokens, startToken, endToken );
    String fieldName = getFieldNameFor( phrase );
    if (fieldName != null) return fieldName;
      
    String stemmed = getStemmed( phrase );
    Log.debug( "checking stemmed " + stemmed );
    return (stemmed.equals( phrase )) ? null : getFieldNameFor( stemmed );
  }
    
  private String getSingleTermQuery( String multiTermValue ) {
        
    String multiTerm = multiTermValue;
    if (multiTermValue.startsWith( "\"" )) {
      multiTerm = new String( multiTermValue.substring( 1, multiTermValue.lastIndexOf( "\"" )));
    }
    Log.debug( "getSingleTermQuery " + multiTerm + "" );
        
    try {
      StringBuilder strb = new StringBuilder( );
            
      String[] terms = multiTerm.split( " " );
      for (int i = 0; i < terms.length; i++) {
        if (i > 0) strb.append( " AND " );
                
        String fieldName = getFieldNameFor( terms[i].toLowerCase( ) );
        Log.debug( "fieldName for " + terms[i].toLowerCase( ) + " is " + fieldName );
        if (fieldName == null) return null;
                
        if (fieldName.indexOf( fieldDelim ) > 0) {
          String[] fields = fieldName.split( fieldSplitExpr );
          strb.append( "(" );
          for (int f = 0; f < fields.length; f++) {
            if (f > 0) strb.append( " OR " );
            strb.append( fields[f] ).append( ":" ).append( getMappedFieldName( termMap, terms[i].toLowerCase( ) ) );
          }
          strb.append( ")" );
        }
        else {
          strb.append( fieldName ).append( ":" ).append( getMappedFieldName( termMap, terms[i].toLowerCase( ) ) );
        }
      }
            
      Log.debug( "getSingleTermQuery returns: '" + strb.toString( ) + "'" );
      return strb.toString( );
    }
    catch (IOException ioe ) {
      return null;
    }
  }
    
  private String getFieldNameFor( String phrase )  throws IOException {
    return ("*".equals( phrase) || "* *".equals( phrase )) ? null : getMappedFieldName( fieldMap, phrase );
  }

    
  // TODO: Return comma separated string if more than one
  private String getMappedFieldName( SynonymMap termMap, String phrase ) throws IOException {
    Log.debug( "getMappedFieldName: '" + phrase + "'" );
    FST<BytesRef> fst = termMap.fst;
    FST.BytesReader fstReader = fst.getBytesReader();
    FST.Arc<BytesRef> scratchArc = new FST.Arc<>( );
    BytesRef scratchBytes = new BytesRef();
    CharsRefBuilder scratchChars = new CharsRefBuilder();
    ByteArrayDataInput bytesReader = new ByteArrayDataInput();
        
    BytesRef pendingOutput = fst.outputs.getNoOutput();
    fst.getFirstArc( scratchArc );
    BytesRef matchOutput = null;
      
    String noSpPhrase = phrase.replace( ' ', '_' );
    int charPos = 0;
    while(charPos < noSpPhrase.length()) {
      final int codePoint = noSpPhrase.codePointAt( charPos );
      if (fst.findTargetArc( codePoint, scratchArc, scratchArc, fstReader) == null) {
        Log.debug( "No FieldName for " + phrase );
        return null;
      }
                
      pendingOutput = fst.outputs.add(pendingOutput, scratchArc.output);
      charPos += Character.charCount(codePoint);
    }

    if (scratchArc.isFinal()) {
      Log.debug( "creating matchOutput" );
      matchOutput = fst.outputs.add(pendingOutput, scratchArc.nextFinalOutput);
      ArrayList<String> mappedFields = new ArrayList<String>( );
      bytesReader.reset( matchOutput.bytes, matchOutput.offset, matchOutput.length );
            
      final int code = bytesReader.readVInt();
      final int count = code >>> 1;
      for( int outputIDX = 0; outputIDX < count; outputIDX++ ) {
        termMap.words.get( bytesReader.readVInt(), scratchBytes );
        scratchChars.copyUTF8Bytes(scratchBytes);
        int lastStart = 0;
        final int chEnd = lastStart + scratchChars.length();
        for( int chIDX = lastStart; chIDX <= chEnd; chIDX++ ) {
          if (chIDX == chEnd || scratchChars.charAt(chIDX) == SynonymMap.WORD_SEPARATOR) {
            int outputLen = chIDX - lastStart;
            assert outputLen > 0: "output contains empty string: " + scratchChars;
            mappedFields.add( new String( scratchChars.chars(), lastStart, outputLen ) );
            lastStart = chIDX + 1;
          }
        }
      }

      if (mappedFields.size() == 1) {
        Log.debug( "returning mapped fieldName " + mappedFields.get( 0 ) );
        return mappedFields.get( 0 );
      }
      else {
        StringBuilder fieldBuilder = new StringBuilder( );
        for (String fieldName : mappedFields ) {
          if (fieldBuilder.length() > 0) fieldBuilder.append( fieldDelim );
          fieldBuilder.append( fieldName );
        }
        Log.debug( "returning mapped fieldName " + fieldBuilder.toString( ) );
        return fieldBuilder.toString( );
      }
    }
      
    Log.warn( "matchOutput but no FieldName for " + phrase );
    return null;
  }

    
  private void buildFieldMap( ResponseBuilder rb ) throws IOException {
    Log.debug( "buildFieldMap" );
    SolrIndexSearcher searcher = rb.req.getSearcher();
    // build a synonym map from the SortedDocValues -
    // for each field value: lower case, stemmed, lookup synonyms from synonyms.txt - map to fieldValue
    SynonymMap.Builder fieldBuilder = new SynonymMap.Builder( true );
    SynonymMap.Builder termBuilder = new SynonymMap.Builder( true );
      
    HashMap<String,UninvertingReader.Type> fieldTypeMap = new HashMap<String,UninvertingReader.Type>( );
      
    ArrayList<String> searchFields = getStringFields( searcher );
    for (String searchField : searchFields ) {
      fieldTypeMap.put( searchField, UninvertingReader.Type.SORTED_SET_BINARY);
    }
    UninvertingReader unvRead = new UninvertingReader( searcher.getLeafReader( ), fieldTypeMap );
  
    for (String searchField : searchFields ) {
      Log.debug( "adding searchField " + searchField );
      CharsRef fieldChars = new CharsRef( searchField );
      SortedSetDocValues sdv = unvRead.getSortedSetDocValues( searchField );
      if (sdv == null) continue;
      Log.debug( "got SortedSetDocValues for " + searchField );
      TermsEnum te = sdv.termsEnum();
      while (te.next() != null) {
        BytesRef term = te.term();
        String fieldValue = term.utf8ToString( );
        addTerm ( fieldChars, fieldValue, fieldBuilder, termBuilder );
      }
    }
      
    addDistributedTerms( rb, fieldBuilder, termBuilder, searchFields );
      
    fieldMap = fieldBuilder.build( );
    termMap = termBuilder.build( );
  }
    
  // TODO: Filter this by the configuration fields ...
  private ArrayList<String> getStringFields( SolrIndexSearcher searcher ) {
    IndexSchema schema = searcher.getSchema();
    ArrayList<String> strFields = new ArrayList<String>( );
      
    Collection<String> fieldNames = searcher.getFieldNames();
    Iterator<String> fnIt = fieldNames.iterator();
    while ( fnIt.hasNext() ) {
      String fieldName = fnIt.next( );
      if (excludeFields == null || !excludeFields.contains( fieldName )) {
        SchemaField field = schema.getField(fieldName);
        if (field.stored() && field.getType() instanceof StrField ) {
          strFields.add( fieldName );
        }
      }
    }
      
    return strFields;
  }
    
  private void addTerm( CharsRef fieldChars, String fieldValue, SynonymMap.Builder fieldBuilder, SynonymMap.Builder termBuilder ) throws IOException {
    
    Log.debug( "got fieldValue: '" + fieldValue + "'" );
    String nospVal = fieldValue.replace( ' ', '_' );
    Log.debug( "got nspace: '" + nospVal + "'" );
    CharsRef nospChars = new CharsRef( nospVal );
    CharsRef valueChars = new CharsRef( fieldValue );
        
    fieldBuilder.add( nospChars, fieldChars, false );
    termBuilder.add( nospChars, valueChars, false );
        
    // lower case term,
    String lowercase = nospVal.toLowerCase( );
    CharsRef lcChars = new CharsRef( lowercase );
    fieldBuilder.add( lcChars, fieldChars, false );
    termBuilder.add( lcChars, valueChars, false );
        
    // stem it
    String stemmed = getStemmed( nospVal );
    if (stemmed.equals( fieldValue ) == false) {
      Log.debug( "adding stemmed: " + stemmed );
      CharsRef stChars = new CharsRef( stemmed );
      fieldBuilder.add( stChars, fieldChars, false );
      termBuilder.add( stChars, valueChars, false );
    }
        
    if (this.synonyms != null) {
      // get synonyms from synonyms.txt
      ArrayList<String> synonymLst = getSynonymsFor( this.synonyms, fieldValue );
      if ( synonymLst != null ) {
        for (String synonym : synonymLst ) {
          String nospSyn = synonym.replace( ' ', '_' );
          Log.debug( "adding: " + synonym + " -> " + fieldValue );
          CharsRef synChars = new CharsRef( nospSyn );
          fieldBuilder.add( synChars, fieldChars, false );
          termBuilder.add( synChars, valueChars, false );
        }
      }
      synonymLst = getSynonymsFor( this.synonyms, fieldValue.toLowerCase() );
      if ( synonymLst != null ) {
        for (String synonym : synonymLst ) {
          String nospSyn = synonym.replace( ' ', '_' );
          Log.debug( "adding: " + synonym + " -> " + fieldValue );
          CharsRef synChars = new CharsRef( nospSyn );
          fieldBuilder.add( synChars, fieldChars, false );
          termBuilder.add( synChars, valueChars, false );
        }
      }
    }
  }
    
  private void addDistributedTerms( ResponseBuilder rb, SynonymMap.Builder fieldBuilder, SynonymMap.Builder termBuilder, ArrayList<String> searchFields ) throws IOException {
    SolrIndexSearcher searcher = rb.req.getSearcher();
    CoreContainer container = searcher.getCore().getCoreDescriptor().getCoreContainer();
      
    ShardHandlerFactory shardHandlerFactory = container.getShardHandlerFactory( );
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    shardHandler.checkDistributed( rb );
      
    Log.debug( "Is Distributed = " + rb.isDistrib );
      
    if( rb.isDistrib ) {
      // create a ShardRequest that contains a Terms Request.
      // don't send to this shard???
      ShardRequest sreq = new ShardRequest();
      sreq.purpose = ShardRequest.PURPOSE_GET_TERMS;
      sreq.actualShards = rb.shards;
      ModifiableSolrParams params = new ModifiableSolrParams( );
        
      params.set( TermsParams.TERMS_LIMIT, -1);
      params.set( TermsParams.TERMS_SORT, TermsParams.TERMS_SORT_INDEX);
      String[] fields = searchFields.toArray( new String[ searchFields.size( )] );
      params.set( TermsParams.TERMS_FIELD, fields );
        
      params.set( CommonParams.DISTRIB, "false" );
      params.set( ShardParams.IS_SHARD, true );
      params.set( ShardParams.SHARDS_PURPOSE, sreq.purpose );
      params.set( CommonParams.QT, termsHandler );
      params.set( TermsParams.TERMS, "true" );
        
      if (rb.requestInfo != null) {
        params.set("NOW", Long.toString(rb.requestInfo.getNOW().getTime()));
      }
      sreq.params = params;
        
      for (String shard : rb.shards ) {
        Log.debug( "sending request to shard " + shard );
        params.set(ShardParams.SHARD_URL, shard );
        shardHandler.submit( sreq, shard, params );
      }
        
      ShardResponse rsp = shardHandler.takeCompletedIncludingErrors( );
      if (rsp != null) {
        Log.debug( "got " + rsp.getShardRequest().responses.size( ) + " responses" );
        for ( ShardResponse srsp : rsp.getShardRequest().responses ) {
          Log.debug( "Got terms response from " + srsp.getShard( ));
        
          if (srsp.getException() != null) {
            Log.debug( "ShardResponse Exception!! " + srsp.getException( ) );
          }
        
          @SuppressWarnings("unchecked")
          NamedList<NamedList<Number>> terms = (NamedList<NamedList<Number>>) srsp.getSolrResponse().getResponse().get("terms");
          if (terms != null) {
            addTerms( terms, fieldBuilder, termBuilder, searchFields );
          }
          else {
            Log.warn( "terms was NULL! - make sure that /terms request handler is defined in solrconfig.xml" );
          }
        }
      }
    }
  }
    
  private void addTerms( NamedList<NamedList<Number>> terms, SynonymMap.Builder fieldBuilder, SynonymMap.Builder termBuilder, ArrayList<String> searchFields ) throws IOException {
    TermsResponse termsResponse = new TermsResponse( terms );
    for (String fieldName : searchFields ) {
      CharsRef fieldChars = new CharsRef( fieldName );
      List<TermsResponse.Term> termList = termsResponse.getTerms( fieldName );
      if (termList != null) {
        for (TermsResponse.Term tc : termList) {
          String term = tc.getTerm();
          Log.debug( "Add distributed term: " + fieldName + " = " + term );
          addTerm( fieldChars, term, fieldBuilder, termBuilder );
        }
      }
    }
  }
    
      
  private ArrayList<String> getSynonymsFor( SynonymMap synMap, String term ) throws IOException {
    Log.debug( "getSynonymsFor '" + term + "'" );
          
    FST<BytesRef> fst = synMap.fst;
    FST.BytesReader fstReader = fst.getBytesReader();
    FST.Arc<BytesRef> scratchArc = new FST.Arc<>( );
    BytesRef scratchBytes = new BytesRef();
    CharsRefBuilder scratchChars = new CharsRefBuilder();
    ByteArrayDataInput bytesReader = new ByteArrayDataInput();
          
    BytesRef pendingOutput = fst.outputs.getNoOutput();
    fst.getFirstArc( scratchArc );
    BytesRef matchOutput = null;
          
    String[] tokens = term.split( " " );
    for (int i = 0; i < tokens.length; i++) {
              
      int charPos = 0;
      while( charPos < tokens[i].length() ) {
        final int codePoint = tokens[i].codePointAt( charPos );
        if (fst.findTargetArc( codePoint, scratchArc, scratchArc, fstReader) == null) {
          Log.debug( "No Synonym for " + term );
          return null;
        }
                  
        pendingOutput = fst.outputs.add(pendingOutput, scratchArc.output);
        charPos += Character.charCount(codePoint);
      }
              
      if (scratchArc.isFinal()) {
        matchOutput = fst.outputs.add(pendingOutput, scratchArc.nextFinalOutput);
      }
              
      if (i < tokens.length-1 && fst.findTargetArc(SynonymMap.WORD_SEPARATOR, scratchArc, scratchArc, fstReader) != null) {
        pendingOutput = fst.outputs.add(pendingOutput, scratchArc.nextFinalOutput);
      }
    }
          
    if (matchOutput != null) {
      ArrayList<String> synonymLst = new ArrayList<String>( );
      bytesReader.reset( matchOutput.bytes, matchOutput.offset, matchOutput.length );
              
      final int code = bytesReader.readVInt();
      final int count = code >>> 1;
      for( int outputIDX = 0; outputIDX < count; outputIDX++ ) {
        synMap.words.get( bytesReader.readVInt(), scratchBytes);
        scratchChars.copyUTF8Bytes(scratchBytes);
        int lastStart = 0;
        final int chEnd = lastStart + scratchChars.length();
        for( int chIDX = lastStart; chIDX <= chEnd; chIDX++ ) {
          if (chIDX == chEnd || scratchChars.charAt(chIDX) == SynonymMap.WORD_SEPARATOR) {
            int outputLen = chIDX - lastStart;
            assert outputLen > 0: "output contains empty string: " + scratchChars;
            String synonym = new String( scratchChars.chars(), lastStart, outputLen );
            Log.debug( "got synonym '" + synonym + "'" );
            synonymLst.add( synonym );
            lastStart = chIDX + 1;
          }
        }
      }
              
      return synonymLst;
    }
          
    return null;
  }

    
  // assume English for now ...
  private String getStemmed( String input ) {
    char[] inputChars = input.toCharArray( );
        
    int lastCh = stem( inputChars, inputChars.length );
    if (lastCh < inputChars.length) {
      return new String( inputChars, 0, lastCh );
    }
        
    return input;
  }
    
  // similar to EnglishMinimalStemmer - fixes "...hes" as in batches couches
  public int stem(char s[], int len) {
    if (len < 3 || s[len-1] != 's')
      return len;
        
    switch(s[len-2]) {
      case 'u':
      case 's': return len;
      case 'e':
        if (len > 3 && s[len-3] == 'i' && s[len-4] != 'a' && s[len-4] != 'e') {
          s[len - 3] = 'y';
          return len - 2;
        }
        if (len > 3 && s[len-3] == 'h') {
          return len-2;
        }
        if (s[len-3] == 'i' || s[len-3] == 'a' || s[len-3] == 'o' || s[len-3] == 'e')
          return len; /* intentional fallthrough */
      default: return len - 1;
    }
  }
    
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    // do nothing - needed so we don't execute the query here.
  }
    
}
