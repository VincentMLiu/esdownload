package com.xixi.esdownload;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Hello world!
 *
 */
public class App 
{
	
	private static RestClient restClient;
	
	
	public static File file;
	
    public static void main( String[] args )  {
    	file = new File("D:\\Desktop\\app_url20181107.log");
    	FileOutputStream fos = null;
    	try {
    		
        	String index = "app_url20181107";
        	
        	 fos = new FileOutputStream(file);
        	
        	restClient = RestClient.builder(
        	        new HttpHost("39.105.82.158", 9200, "http"),
        	        new HttpHost("39.105.90.42", 9200, "http"),
        	        new HttpHost("39.105.92.61", 9200, "http")
        	        ).build();
    		
  
        	
        	
        	String returnScrollId = search(index , "", fos);
        	while(!returnScrollId.equals("")) {
        		System.out.println(returnScrollId);
        		returnScrollId = search(index, returnScrollId, fos);
        	}
        	fos.flush();
        	fos.close();
			restClient.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    
    public static String  search(String index, String scrollId, FileOutputStream fos) throws IOException {
    	
    	
    	String returnScrollId = "";
        Map<String, String> params = new HashMap<String, String>();

        params.put("scroll", "1m");
//        params.put("search_type", "scan");
//        params.put("size", "2000");//每次返回总数 = size
        
        String queryString = "{\"size\" : 2000}";
//        if(scrollId!=null && !scrollId.equals("")) {
//        	params.put("scroll_id", "\'" + scrollId + "\'");
//        }
//        queryString = scrollId;
        

        HttpEntity entity = new NStringEntity(queryString, ContentType.APPLICATION_JSON);

        try {

        	Response response;
            if(scrollId!=null && !scrollId.equals("")) {
//            	response =  restClient.performRequest("GET", "/"+ index+ "/_search/scroll/", params, entity);
            	response = restClient.performRequest("GET", "/_search/scroll/?scroll=1m&scroll_id=" + new String(scrollId.getBytes(), "UTF-8") + "");
            }else {
            	response =  restClient.performRequest("GET", "/"+ index+ "/_search", params, entity);
            	
            }
        System.out.println("response status:" + response.getStatusLine().getStatusCode());
        String responseBody = null;

        responseBody = EntityUtils.toString(response.getEntity());
        System.out.println("******************************************** ");

        JSONObject jsonObject = JSON.parseObject(responseBody);

        
        JSONObject hitsObject =  (JSONObject) jsonObject.get("hits");
        BigInteger total = hitsObject.getBigInteger("total");
        
        JSONArray hits = hitsObject.getJSONArray("hits");
        int dealingNum = hits.size();
        if(dealingNum > 0 ) {
        	returnScrollId =  jsonObject.get("_scroll_id").toString();
            StringBuffer sb = new StringBuffer();
            
            for(Object doc : hits) {
            	//写文件
            	sb.append(doc.toString() + "\r\n");
//            	System.out.println(doc.toString());
            }
            
            fos.write(sb.toString().getBytes());
            
        }else {
        	System.out.println("########end of searching########");
        }

        }catch (ResponseException e){
        	e.printStackTrace();
        	//
        	System.out.println("maybe done, maybe not");
        }
        
        
        return returnScrollId;
        
    }
    
}
