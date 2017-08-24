package org.apache.solr.handler.component;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;


public class DistributedQueryAutoFilteringTest extends BaseDistributedSearchTestCase {

  public DistributedQueryAutoFilteringTest() {
    stress = 0;
  }
    
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCore( "solrconfig-autofilter.xml", "schema-autofilter.xml" );
  }
    
  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    del("*:*");
    
    index( id, "1", "color", "red",   "product", "shoes" );
    index( id, "2", "color", "red",   "product", "socks" );
    index( id, "3", "color", "brown", "product", "socks" );
    index( id, "4", "color", "green", "brand", "red lion", "product", "socks" );
    index( id, "5", "color", "blue",  "brand", "red lion", "product", "socks" );
    index( id, "6", "color", "blue",  "brand", "red dragon", "product", "socks" );
    index( id, "7", "brand", "red baron", "product", "pizza" );
    index( id, "8", "brand", "red label", "product", "whiskey" );
    index( id, "9", "brand", "red light", "product", "smoke detector" );
    index( id, "10", "brand", "red star", "product", "yeast" );
    index( id, "11", "brand", "gallo", "product", "red wine" );
    index( id, "12", "brand", "heinz", "product", "red wine vinegar" );
    index( id, "13", "brand", "dole",  "product", "red grapes" );
    index( id, "14", "brand", "acme",  "product", "red brick" );
    commit();
      
    handle.put("distrib", SKIP);
    handle.put("shards", SKIP);
      
    QueryResponse rsp;
    rsp = query( CommonParams.Q, "red lion socks", "fl", "id", "rows", 20, "qt", "/select", "sort", "id asc" );
    assertFieldValues(rsp.getResults(), id, "1", "10", "11", "12", "13", "14", "2", "3", "4", "5", "6", "7", "8", "9"  );
      
    rsp = query( CommonParams.Q, "red lion socks", "fl", "id", "qt", "/autofilter", "sort", "id asc" );
    assertFieldValues(rsp.getResults(), id, "4", "5" );
      
    rsp = query( CommonParams.Q, "blue red lion socks", "fl", "id", "qt", "/autofilter" );
    assertFieldValues(rsp.getResults(), id, "5" );
      
    rsp = query( CommonParams.Q, "red wine", "fl", "id", "qt", "/autofilter" );
    assertFieldValues(rsp.getResults(), id, "11" );
      
    rsp = query( CommonParams.Q, "red wine vinegar", "fl", "id", "qt", "/autofilter" );
    assertFieldValues(rsp.getResults(), id, "12" );
  }
   
  @Override
  protected QueryResponse query(Object... q) throws Exception {
        
    final ModifiableSolrParams params = new ModifiableSolrParams();
        
    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }
    params.set("shards", getShardsString());
      
    return queryServer(params);
  }
}
