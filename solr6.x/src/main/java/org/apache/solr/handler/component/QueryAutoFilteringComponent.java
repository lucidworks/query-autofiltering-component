package org.apache.solr.handler.component;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.*;

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
    
  private HashSet<String> whitelistFields;;
  private HashSet<String> excludeFields;
  private HashSet<String> stopwords;
    
  private Integer boostFactor;  // if null, use Filter Query
    
  // For multiple terms in the same field, if field is multi-valued = use AND for filter query
  private boolean useAndForMultiValuedFields = true;

  private String fieldDelim = "|";
    
  private String fieldSplitExpr = "\\|";
    
  // map of a "verb" phrase to a metadata field
  private ArrayList<ModifierDefinition> verbModifierList;
    
  @Override
  public void init( NamedList initArgs ) {
    List<String> whitelistFields = (List<String>) initArgs.get("whitelistFields");
    if (whitelistFields != null) {
      this.whitelistFields = new HashSet<String>( );
      for (String field : whitelistFields ) {
          this.whitelistFields.add( field );
      }
    }

    List<String> excludeFields = (List<String>) initArgs.get("excludeFields");
    if (excludeFields != null) {
      this.excludeFields = new HashSet<String>( );
      for (String field : excludeFields ) {
          this.excludeFields.add( field );
      }
    }

    List<String> verbModifiers = (List<String>)initArgs.get( "verbModifiers" );
    if (verbModifiers != null) {
      this.verbModifierList = new ArrayList<ModifierDefinition>( );
      for (String modifier : verbModifiers) {
        String modifierPhrase = new String( modifier.substring( 0, modifier.indexOf( ":" )));
        String modifierFields = new String( modifier.substring( modifier.indexOf( ":" ) + 1 ));
              
        if (modifierPhrase.indexOf( "," ) > 0) {
          String[] phrases = modifierPhrase.split( "," );
          for (int i = 0; i < phrases.length; i++) {
            addModifier( phrases[i], modifierFields );
          }
        }
        else {
          addModifier( modifierPhrase, modifierFields );
        }
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
    
  private void addModifier( String modifierPhrase, String modifierFields ) {
    Log.info( "addModifier: " + modifierPhrase + ": " + modifierFields );
    ModifierDefinition modDef = new ModifierDefinition( );
    modDef.modifierPhrase = modifierPhrase.toLowerCase( );
        
    if (modifierFields.indexOf( fieldDelim ) > 0) {
      modDef.filterFields = new HashMap<String,String>( );
      String fieldPairs = new String( modifierFields.substring( modifierFields.indexOf( fieldDelim ) + 1 ));
      modifierFields = new String( modifierFields.substring( 0, modifierFields.indexOf( fieldDelim )));
      Log.info( "fieldPairs = " + fieldPairs );
            
      String modifierTemplate = null;
      if (fieldPairs.indexOf( fieldDelim ) > 0) {
        modifierTemplate = new String( fieldPairs.substring( fieldPairs.indexOf( fieldDelim ) + 1 ));
        fieldPairs = new String( fieldPairs.substring( 0, fieldPairs.indexOf( fieldDelim )));
      }
            
      if (fieldPairs.indexOf( "," ) > 0) {
        String[] fieldPairList = fieldPairs.split( "," );
        for (int i = 0; i < fieldPairList.length; i++) {
          String field = new String( fieldPairList[i].substring( 0, fieldPairList[i].indexOf( ":" )));
          String value = new String(fieldPairList[i].substring( fieldPairList[i].indexOf( ":" ) + 1 ));
          modDef.filterFields.put( field, value );
        }
      }
      else {
        String field = new String(fieldPairs.substring( 0, fieldPairs.indexOf( ":" )));
        String value = new String( fieldPairs.substring( fieldPairs.indexOf( ":" ) + 1 ));
        modDef.filterFields.put( field, value );
      }
            
      if (modifierTemplate != null) {
        modDef.templateRule = new ModifierTemplateRule( modifierTemplate );
      }
    }
    modDef.modifierFields = new ArrayList<String>( );
    if (modifierFields.indexOf( "," ) > 0) {
      String[] fields = modifierFields.split( "," );
      for (int i = 0; i < fields.length; i++) {
        modDef.modifierFields.add( fields[i] );
      }
    }
    else {
      modDef.modifierFields.add( modifierFields );
    }
        
    modDef.modTokens = modDef.modifierPhrase.split( " " );
    verbModifierList.add( modDef );
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
      
    Log.info( "prepare ..." );
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
    ArrayList<char[]> queryTokens = tokenize( qStr );
      
    if (queryTokens.size( ) >= mintok) {
      ModifiableSolrParams modParams = new ModifiableSolrParams( params );
      if (findPattern( queryTokens, rb, modParams )) {
        req.setParams( modParams );
      }
    }
  }

  /**
   * If this method is not overridden then this will cause a request against
   * the Shards causing performance degredation and duplicate values in the
   * facet counts.
   * Here we just return that this is done leaving it up to the Query to drive
   * the requests.
   *
   * @param rb Ignored
   * @return ResponseBuilder.STAGE_DONE
   * @throws IOException never thrown
   */
  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return ResponseBuilder.STAGE_DONE;
  }

  private boolean findPattern( ArrayList<char[]> queryTokens, ResponseBuilder rb, ModifiableSolrParams modParams ) throws IOException {
    Log.debug( "findPattern " );

    HashSet<Integer> usedTokens = new HashSet<Integer>( );
    HashMap<String,ArrayList<String>> fieldMap = new HashMap<String,ArrayList<String>>( );
    HashMap<String,int[]> fieldPositionMap = new HashMap<String,int[]>( );
    HashMap<String,int[]> entityPositionMap = (verbModifierList != null) ? new HashMap<String,int[]>()  : null;

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
        String indexedTerm = getMappedFieldName( termMap, phrase.toLowerCase( ));
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

          Log.info( "indexedTerm: " + indexedTerm );
          int[] entityPosition = null;
          if (entityPositionMap != null) {
            entityPosition = new int[2];
            entityPosition[0] = startToken;
            entityPosition[1] = endToken-1;
          }

          Log.debug( "indexedTerm: " + indexedTerm );
          if (indexedTerm.indexOf( fieldDelim ) > 0)
          {
            String[] indexedTerms = indexedTerm.split( fieldSplitExpr );
            for (int t = 0; t < indexedTerms.length; t++) {
              valList.add( indexedTerms[t] );
              if (entityPositionMap != null) entityPositionMap.put( indexedTerms[t], entityPosition );
            }
          }
          else {
            valList.add( indexedTerm );
            if (entityPositionMap != null) entityPositionMap.put( indexedTerm, entityPosition );
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

      // filter field maps based on verbs here:
      if (entityPositionMap != null) {
        filterFieldMap( queryTokens, fieldMap, entityPositionMap, fieldPositionMap );
      }

      String useBoost = modParams.get( BOOST_PARAM );
      Integer boostFactor = (useBoost != null) ? new Integer( useBoost ) : this.boostFactor;
      if (boostFactor == null) {
        StringBuilder qbuilder = new StringBuilder( );
        if (usedTokens.size( ) < queryTokens.size( ) ) {
          for (int i = 0; i < queryTokens.size(); i++) {
            if (boostFactor != null || usedTokens.contains( new Integer( i ) ) == false ) {
              char[] token = queryTokens.get( i );
              if (qbuilder.length() > 0) qbuilder.append( " " );
              qbuilder.append( token );
            }
          }
        }

        Log.debug( "got qbuilder string = '" + qbuilder.toString() + "'" );
        if (qbuilder.length() == 0 && fieldMap.size() > 0) {
          // build a filter query -
          // EH: can't do this if dismax
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
        // use the bq field to add fielded boost clauses
        StringBuilder bbuilder = new StringBuilder( );
        String boostSuffix = "^" + boostFactor.toString( );
        for (String fieldName : fieldMap.keySet( ) ) {
          bbuilder.append( " " );
          bbuilder.append( getFilterQuery( rb, fieldName, fieldMap.get( fieldName ), fieldPositionMap.get( fieldName ), queryTokens, boostSuffix ) );
        }
        Log.info( "adding bq = '" + bbuilder.toString()  + "'" );
        modParams.add( "bq", bbuilder.toString( ).trim() );
      }
      return true;
    }

    return false;
  }
    
  private String getPhrase( ArrayList<char[]> tokens, int startToken, int endToken ) {
    return getPhrase( tokens, startToken, endToken, "_" );
  }
    
  private String getPhrase( ArrayList<char[]> tokens, int startToken, int endToken, String tokenSep ) {
    StringBuilder strb = new StringBuilder( );
    for (int i = startToken; i <= endToken; i++) {
      if (i > startToken) { strb.append( tokenSep ); }

      strb.append( tokens.get( i ) );
    }
      Log.debug( "getPhrase returns " + strb.toString( ) );
    return strb.toString( );
  }
    
  private String getFilterQuery( ResponseBuilder rb, String fieldName, ArrayList<String> valList,
                                 int[] termPosRange, ArrayList<char[]> queryTokens, String suffix) {
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
          // EH: possible meta-escaping problem if value includes {!field f=<fieldName>}value
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
          char[] qToken = queryTokens.get( i );
          // is the token 'or'?
          if (qToken.length == 2 && qToken[0] == 'o' && qToken[1] == 'r' ) {
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
                                 int[] termPosRange, ArrayList<char[]> queryTokens, String suffix) {
    StringBuilder filterQBuilder = new StringBuilder( );
    for (int i = 0; i < fieldNames.length; i++) {
      if (i > 0) filterQBuilder.append( " OR " );
      filterQBuilder.append( getFilterQuery( rb, fieldNames[i], valList, termPosRange, queryTokens, suffix ) );
    }
    return "(" + filterQBuilder.toString() + ")";
  }
    
  private String getFieldNameFor( ArrayList<char[]> queryTokens, int startToken, int endToken ) throws IOException {
    String phrase = getPhrase( queryTokens, startToken, endToken );
    String fieldName = getFieldNameFor( phrase.toLowerCase( ) );
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
    Log.debug( "getFieldNameFor '" + phrase + "'" );
    return ("*".equals( phrase) || "* *".equals( phrase )) ? null : getMappedFieldName( fieldMap, phrase.toLowerCase( ) );
  }

    
  // TODO: Return comma separated string if more than one
  private String getMappedFieldName( SynonymMap termMap, String phrase ) throws IOException {
    Log.debug( "getMappedFieldName: '" + phrase + "'" );
    FST<BytesRef> fst = termMap.fst;
    if(fst != null) {
      FST.BytesReader fstReader = fst.getBytesReader();
      FST.Arc<BytesRef> scratchArc = new FST.Arc<>();
      BytesRef scratchBytes = new BytesRef();
      CharsRefBuilder scratchChars = new CharsRefBuilder();
      ByteArrayDataInput bytesReader = new ByteArrayDataInput();

      BytesRef pendingOutput = fst.outputs.getNoOutput();
      fst.getFirstArc(scratchArc);
      BytesRef matchOutput = null;

      String noSpPhrase = phrase.replace(' ', '_');
      int charPos = 0;
      while (charPos < noSpPhrase.length()) {
        final int codePoint = noSpPhrase.codePointAt(charPos);
        if (fst.findTargetArc(codePoint, scratchArc, scratchArc, fstReader) == null) {
          Log.debug("No FieldName for " + phrase);
          return null;
        }

        pendingOutput = fst.outputs.add(pendingOutput, scratchArc.output);
        charPos += Character.charCount(codePoint);
      }

      if (scratchArc.isFinal()) {
        Log.debug("creating matchOutput");
        matchOutput = fst.outputs.add(pendingOutput, scratchArc.nextFinalOutput);
        ArrayList<String> mappedFields = new ArrayList<String>();
        bytesReader.reset(matchOutput.bytes, matchOutput.offset, matchOutput.length);

        final int code = bytesReader.readVInt();
        final int count = code >>> 1;
        for (int outputIDX = 0; outputIDX < count; outputIDX++) {
          termMap.words.get(bytesReader.readVInt(), scratchBytes);
          scratchChars.copyUTF8Bytes(scratchBytes);
          int lastStart = 0;
          final int chEnd = lastStart + scratchChars.length();
          for (int chIDX = lastStart; chIDX <= chEnd; chIDX++) {
            if (chIDX == chEnd || scratchChars.charAt(chIDX) == SynonymMap.WORD_SEPARATOR) {
              int outputLen = chIDX - lastStart;
              assert outputLen > 0 : "output contains empty string: " + scratchChars;
              mappedFields.add(new String(scratchChars.chars(), lastStart, outputLen));
              lastStart = chIDX + 1;
            }
          }
        }

        if (mappedFields.size() == 1) {
          Log.debug("returning mapped fieldName " + mappedFields.get(0));
          return mappedFields.get(0);
        } else {
          StringBuilder fieldBuilder = new StringBuilder();
          for (String fieldName : mappedFields) {
            if (fieldBuilder.length() > 0) fieldBuilder.append(fieldDelim);
            fieldBuilder.append(fieldName);
          }
          Log.debug("returning mapped fieldName " + fieldBuilder.toString());
          return fieldBuilder.toString();
        }
      }
    } else {
      Log.debug("Finite State Machine is null on Synonym Map -> ignored");
    }
     
    // Surpressing this message since it is very chatty in production. 
    Log.debug( "matchOutput but no FieldName for " + phrase );
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

    ArrayList<String> strFields = new ArrayList<String>( );
    
    if ( hasWhitelist() ) {
      Log.info("Using whitelist fields instead of schema.");
      for ( String fieldName: whitelistFields ) {
        strFields.add( fieldName );
      }
    } else {
      IndexSchema schema = searcher.getSchema();
      Iterable<String> fieldNames = searcher.getFieldNames();
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
    }
    
    return strFields;
  }

  private boolean hasWhitelist() {
    return this.whitelistFields != null && this.whitelistFields.size() > 0;
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
 
    final SolrParams distribParams = rb.req.getParams(); 
    final boolean isDistrib = distribParams.get(ShardParams.SHARDS) != null;
    Log.debug( "Is Distributed = " + isDistrib );
      
    if( isDistrib ) {
      shardHandler.prepDistributed( rb );
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
    
  private ArrayList<char[]> tokenize( String input ) throws IOException {

    Log.debug( "tokenize '" + input + "'" );
    ArrayList<char[]> tokens = new ArrayList<char[]>( );
    Tokenizer tk = getTokenizerImpl( input );

    CharTermAttribute term = tk.addAttribute( CharTermAttribute.class );
    tk.reset( );
    while (tk.incrementToken( ) ) {
      int bufLen = term.length();
      char[] copy = new char[ bufLen ];
      System.arraycopy(term.buffer( ), 0, copy, 0, bufLen );
      tokens.add( copy );
    }

    return tokens;
  }
    
  private Tokenizer getTokenizerImpl( String input ) throws IOException {
    StandardTokenizer sttk = new StandardTokenizer( );
    sttk.setReader( new StringReader( input ) );
    return sttk;
  }
    
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    // do nothing - needed so we don't execute the query here.
  }
    
  // ===========================================================================
  // Verb Modifier Code
  // Using the verb modifier map if a verb modifier is adjacent to a field mapped phrase (can have noise words between)
  // restrict the field names in the list to the one that is linked to the verb modifier
  // TODO - how to deal with 'and' and 'or' Between modifiers
  // ===========================================================================
  private void filterFieldMap( ArrayList<char[]> queryTokens, HashMap<String,ArrayList<String>> fieldMap,
                               HashMap<String,int[]> entityPositionMap, HashMap<String,int[]> fieldPositionMap ) {

    Log.info( "filterFieldMap" );
    // need to find the modifiers that are in THIS set of tokens by position, in the order used ...
    ArrayList<ModifierInstance> usedModifiers = getOrderedModifierPositions( queryTokens );
    if (usedModifiers == null || usedModifiers.size() == 0) {
      return; // nothing to do ...
    }
        
    // find the verb modifiers in the query tokens list
    // need to keep track of 'next entity' and 'last entity' as we iterate
    boolean remapped = false;
    for (ModifierInstance modInstance : usedModifiers) {
      if (modInstance.templateRule != null) applyModifierTemplateRule( entityPositionMap, fieldMap, modInstance.templateRule );
            
      HashMap<String,String> fieldNameKeys = getFieldKeysForFieldName( modInstance.modifierFields, fieldMap );
      if (fieldNameKeys != null) {
        // find the entity just before (maximum pos before) or after (minimum pos after) the modifier phrase from entityPositionMap
        // assumming here that the modifiers can work bi-directionally
        // as in 'songs Paul McCartney composed'  or 'songs Paul McCartney has written' vs. 'songs composed by Paul McCartney'
        // or 'Bands Paul McCartney was in'  vs. 'who was in the Who'
        for (String fieldNameKey : fieldNameKeys.keySet() ) {
          String modifierField = fieldNameKeys.get( fieldNameKey );
                    
          HashSet<String> entityPhrases = findLastEntitiesBefore( entityPositionMap, modInstance, usedModifiers, fieldMap.get( fieldNameKey ) );
          if ( entityPhrases != null ) {
            remapEntity( fieldNameKey, entityPhrases, modifierField, fieldMap, fieldPositionMap, entityPositionMap );
            remapped = true;
          }
          else {
            entityPhrases = findFirstEntitiesAfter( entityPositionMap, modInstance, usedModifiers, fieldMap.get( fieldNameKey ) );
            if (entityPhrases != null) {
              remapEntity( fieldNameKey, entityPhrases, modifierField, fieldMap, fieldPositionMap, entityPositionMap );
              remapped = true;
            }
          }
        }
      }
            
      // add any filter fields for the verbs:
      if (remapped && modInstance.filterFields != null) {
        Log.info( "checking verb modifiers for " + modInstance.modifierFields );
        for (String filtField : modInstance.filterFields.keySet( ) ) {
          ArrayList<String> valList = new ArrayList<String>( );
          valList.add( modInstance.filterFields.get( filtField ) );
          Log.info( "setting verb filter: " + filtField + ":" + modInstance.filterFields.get( filtField ) );
          fieldMap.put( filtField, valList );
          fieldPositionMap.put( filtField, modInstance.modifierPos );
        }
      }
    }
  }
    
  private ArrayList<ModifierInstance> getOrderedModifierPositions( ArrayList<char[]> queryTokens ) {
    ArrayList<ModifierInstance> modifiers = null;
    int i = 0;
    while (i < queryTokens.size( ) ) {
      char[] token = queryTokens.get( i );
      ModifierDefinition modifier = findModifier( token );
      if (modifier != null && matchesModifier( modifier.modTokens, queryTokens, i )) {
        Log.info( "Adding Modifier Instance '" + modifier.modifierPhrase + "'" );
        ModifierInstance modInst = new ModifierInstance( );
        modInst.modifierPhrase = modifier.modifierPhrase;
        modInst.modifierFields = modifier.modifierFields;
        Log.info( "fields: " );
        for (String modField : modifier.modifierFields ) { Log.info( "   " + modField ); }
        modInst.modifierPos = new int[2];
        modInst.modifierPos[0] = i;
        modInst.modifierPos[1] = i + modifier.modTokens.length - 1;
          
        modInst.filterFields = modifier.filterFields;
        modInst.templateRule = modifier.templateRule;
        if (modifiers == null) modifiers = new ArrayList<ModifierInstance>( );
        modifiers.add( modInst );
        i += modifier.modTokens.length;
      }
      else {
        ++i;
      }
    }
        
    return modifiers;
  }
    
  private ModifierDefinition findModifier( char[] queryToken ) {
    for (ModifierDefinition modifier : verbModifierList ) {
      if (modifier.modifierPhrase.startsWith( new String( queryToken ) )) {
        return modifier;
      }
    }
    return null;
  }
    
  private boolean matchesModifier( String[] modTokens, ArrayList<char[]> queryTokens, int start ) {
    int i = 0;
    while ( (start + i) < queryTokens.size( ) && i < modTokens.length ) {
      String token = new String( queryTokens.get( start + i ) );
      if (!token.toLowerCase( ).equals( modTokens[i].toLowerCase( ))) return false;
      if (++i == modTokens.length) return true;
    }
    return false;
  }
    
    
  private HashMap<String,String> getFieldKeysForFieldName( ArrayList<String> modifierFields, HashMap<String,ArrayList<String>> fieldMap ) {
    Log.info( "getFieldKeysForFieldName" );
    HashMap<String,String> fieldKeys = null;
    for (String modifierField : modifierFields ) {
      Log.info( "testing modifierField: " + modifierField );
      for (String fieldNameList : fieldMap.keySet() ) {
        Log.info( "testing fieldNameList: " + fieldNameList );
        String[] fields = fieldNameList.split( fieldSplitExpr );
        for (int i = 0; i < fields.length; i++) {
          if ( fields[i].equals( modifierField )) {
            if (fieldKeys == null) fieldKeys = new HashMap<String,String>( );
            Log.info( "adding field Key " + fieldNameList + ": " + modifierField );
            fieldKeys.put( fieldNameList, modifierField );
          }
        }
      }
    }
    return fieldKeys;
  }

    
    
  // find entities before the current mod pos but after the last one (if modPos is not first in the list of modifier positions)
  // we also need to keep track of the operator (???)
  private HashSet<String> findLastEntitiesBefore( HashMap<String,int[]> entityPositionMap, ModifierInstance modifier,
                                                  ArrayList<ModifierInstance> usedModifiers, ArrayList<String> fieldVals ) {
    Log.info( "findLastEntitiesBefore" );
    HashSet<String> entitySet = null;
    int previousModifierPosition = -1;
    int thisModPos = modifier.modifierPos[0];
        
    for ( ModifierInstance mod : usedModifiers ) {
      if (mod.modifierPos[1] < thisModPos ) {
        previousModifierPosition = mod.modifierPos[1];
        break;
      }
    }
        
    for (String entityPhrase : entityPositionMap.keySet( ) ) {
      Log.info( " testing " + entityPhrase );
      if (fieldVals.contains( entityPhrase)) {
        int[] entityPos = entityPositionMap.get( entityPhrase );
        Log.info( "entity is at " + entityPos[0] + "," + entityPos[1] );
        Log.info( "mod is at " + thisModPos + " previous mod was " +  previousModifierPosition  );
        if (entityPos[1] < thisModPos && entityPos[0] > previousModifierPosition ) {
          if (entitySet == null) entitySet = new HashSet<String>( );
          Log.info( "adding " + entityPhrase );
          entitySet.add( entityPhrase );
        }
      }
    }
        
    return entitySet;
  }
    
  // find entities after the current mod pos but before the next modifier
  private HashSet<String> findFirstEntitiesAfter( HashMap<String,int[]> entityPositionMap, ModifierInstance modifier,
                                                  ArrayList<ModifierInstance> usedModifiers, ArrayList<String> fieldVals ) {
    Log.info( "findFirstEntitiesAfter" );
    HashSet<String> entitySet = null;
    int nextModifierPosition = Integer.MAX_VALUE;
    int thisModPos = modifier.modifierPos[1];
        
    for (ModifierInstance mod : usedModifiers ) {
      if (mod.modifierPos[0] > thisModPos ) {
        nextModifierPosition = mod.modifierPos[0];
        break;
      }
    }
        
    for (String entityPhrase : entityPositionMap.keySet( ) ) {
      Log.info( " testing " + entityPhrase );
      if (fieldVals.contains( entityPhrase)) {
        int[] entityPos = entityPositionMap.get( entityPhrase );
        Log.info( "entity is at " + entityPos[0] + "," + entityPos[1] );
        Log.info( "mod is at " + thisModPos + " next mod is " +  nextModifierPosition  );
        if (entityPos[0] > thisModPos && entityPos[1] < nextModifierPosition ) {
          if (entitySet == null) entitySet = new HashSet<String>( );
          Log.info( "adding " + entityPhrase );
          entitySet.add( entityPhrase );
        }
      }
    }
        
    return entitySet;
  }
    
    
  private void remapEntity( String fieldNameKey, HashSet<String> entityValues, String modifierField,
                            HashMap<String,ArrayList<String>> fieldMap, HashMap<String,int[]> fieldPositionMap, HashMap<String,int[]> entityPositionMap ) {
    // find the fieldMap key that contains the fieldName
    ArrayList<String> fieldVals = fieldMap.get( fieldNameKey );
        
    boolean allMatch = true;
    for (String fieldVal : fieldVals ) {
      if (!entityValues.contains( fieldVal )) {
        allMatch = false;
        break;
      }
    }
        
    // if the field values in the fieldMap match the set of entity values -- remove the fieldNameKey and replace it with the modifierField in the map
    if ( allMatch ) {
      if (fieldNameKey.equals( modifierField )) return;
        
      fieldMap.remove( fieldNameKey );
      Log.info( "remapping: " + modifierField );
      for( String val : fieldVals ) { Log.info( "    " + val ); }
      fieldMap.put( modifierField, fieldVals );
    }
    else {
      // for a partial map - remove the field values in the fieldMap that are in the entityValues set, and create a new entry with modifierField => entityValues
      ArrayList<String> remaining = new ArrayList<String>( );
      ArrayList<String> modList = new ArrayList<String>( );
      for (String fieldVal : fieldVals ) {
        if (entityValues.contains( fieldVal )) {
          modList.add( fieldVal );
        }
        else {
          remaining.add( fieldVal );
        }
      }
        
      fieldMap.put( modifierField, modList );
      fieldPositionMap.put( modifierField, getPosArrayFor( modList, entityPositionMap ) );
            
      fieldMap.put( fieldNameKey, remaining );
      fieldPositionMap.put( fieldNameKey, getPosArrayFor( remaining, entityPositionMap ) );
    }
  }
    
  private void applyModifierTemplateRule( HashMap<String, int[]> entityPositionMap, HashMap<String,ArrayList<String>> fieldMap, ModifierTemplateRule modifierRule ) {
    Log.info( "applyModifierTemplateRule" );
    // find entity_1_field - from field map - find entityPosition from values
    ArrayList<String> firstEntityList = findEntityList( fieldMap, modifierRule.entity_1_field );
    if (firstEntityList == null) return;
    String firstFieldList = null;
    String entityValue = null;
        
    for (String firstEntity : firstEntityList ) {
      Log.info( "checking entity: " + firstEntity );
      int[] firstPos = entityPositionMap.get( firstEntity );
      int[] secondPos = entityPositionMap.get( modifierRule.entity_2_value );
      if (secondPos != null && (secondPos[0] == firstPos[1] + 1) && findEntityList( fieldMap, modifierRule.entity_2_field ) != null ) {
        if (modifierRule.entity_1_value.equals( "_ENTITY_" )) {
          Log.info( "'" + firstEntity + "' matches pattern" );
          entityValue = firstEntity;
          ArrayList<String> outputList = new ArrayList<String>( );
          outputList.add( firstEntity );
          firstFieldList = findFieldList( fieldMap, modifierRule.entity_1_field );
          fieldMap.put( modifierRule.output_field, outputList );
          break;
        }
      }
    }
        
    if ( firstFieldList != null ) {
      // remove remapped entity field from field list
      Log.info( "removing " + modifierRule.entity_1_field + " from " + firstFieldList );
      String[] fields = firstFieldList.split( "\\|" );
      StringBuilder stb = new StringBuilder( );
      for (int i = 0; i < fields.length; i++) {
        if (fields[i].equals( modifierRule.entity_1_field) == false ) {
          if (stb.length() > 0) stb.append( "," );
          stb.append( fields[i] );
        }
      }
            
      // remove entityValue from fieldMap arrayList
      if (stb.length() > 0) {
        Log.info( "new field list: " + stb.toString( ) );
        ArrayList<String> remainder = new ArrayList<String>( );
        for (String firstEntity : firstEntityList ) {
          if (firstEntity.equals( entityValue ) == false ) {
            Log.info( "adding remaining value " + firstEntity );
            remainder.add( firstEntity );
          }
        }
        if (remainder.size( ) > 0) {
          Log.info( "remainder fields: " + stb.toString( ) );
          fieldMap.put( stb.toString( ), remainder );
        }
                
        Log.info( "removing field: " + firstFieldList );
        fieldMap.remove( firstFieldList );
      }
    }
  }
    
  private ArrayList<String> findEntityList( HashMap<String,ArrayList<String>> fieldMap, String entityField ) {
    for (String fieldList : fieldMap.keySet() ) {
      if (fieldList.contains( entityField )) {
        return fieldMap.get( fieldList );
      }
    }
    return null;
  }
    
  private String findFieldList( HashMap<String,ArrayList<String>> fieldMap, String entityField ) {
    for (String fieldList : fieldMap.keySet() ) {
      if (fieldList.contains( entityField )) {
        return fieldList;
      }
    }
    return null;
  }
    
  private int[] getPosArrayFor( ArrayList<String> entities, HashMap<String,int[]> entityPositionMap ) {
    int[] newPosArray = null;
    for ( String entity : entities ) {
      int[] entityPos = entityPositionMap.get( entity );
      if (entityPos != null) {
        if (newPosArray == null) newPosArray = entityPos;
        else {
          if (entityPos[1] < newPosArray[0] ) {
            newPosArray[0] = entityPos[0];
          }
          if (entityPos[0] > newPosArray[1] ) {
            newPosArray[1] = entityPos[1];
          }
        }
      }
    }
        
    return newPosArray;
  }
    
  private class ModifierDefinition
  {
    String modifierPhrase;  // the phrase that will modify like 'was in'
    ArrayList<String> modifierFields;   // the field(s) that will be used like 'memberOfGroup_ss,groupMembers_ss'
    String[] modTokens;
    HashMap<String,String> filterFields;
    ModifierTemplateRule templateRule;
  }
    
  private class ModifierInstance
  {
    String modifierPhrase;
    ArrayList<String> modifierFields;
    int[] modifierPos;
    HashMap<String,String> filterFields;
    ModifierTemplateRule templateRule;
  }
    
  // original_performer_s:_ENTITY_,recording_type_ss:Song=>original_performer_s:_ENTITY_
  private class ModifierTemplateRule
  {
    String entity_1_field;
    String entity_1_value;
        
    String entity_2_field;
    String entity_2_value;
        
    String output_field;
    String output_value;
        
    ModifierTemplateRule( String templatePattern ) {
      String leftSide = new String(templatePattern.substring( 0, templatePattern.indexOf( "=>" )));
      String rightSide = new String(templatePattern.substring( templatePattern.indexOf( "=>" ) + 2 ));
        
      String entity_1 = new String( leftSide.substring( 0, leftSide.indexOf( "," )));
      String entity_2 = new String( leftSide.substring( leftSide.indexOf( "," ) + 1 ));
        
      entity_1_field = new String( entity_1.substring( 0, entity_1.indexOf( ":" )));
      entity_1_value = new String( entity_1.substring( entity_1.indexOf( ":" ) + 1 ));
      entity_2_field = new String( entity_2.substring( 0, entity_2.indexOf( ":" )));
      entity_2_value = new String( entity_2.substring( entity_2.indexOf( ":" ) + 1 ));
        
      output_field = new String( rightSide.substring( 0, rightSide.indexOf( ":" )));
      output_value = new String( rightSide.substring( rightSide.indexOf( ":" ) + 1 ));
        
      Log.info( "entity_1_field: " + entity_1_field + " entity_1_value: " + entity_1_value );
      Log.info( "entity_2_field: " + entity_2_field + " entity_2_value: " + entity_2_value );
      Log.info( "output_field: " + output_field + " output_value: " + output_value );
    }
  }
    
}
