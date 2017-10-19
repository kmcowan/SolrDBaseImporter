/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package club.capture.dbase.solr;

import club.capture.dbase.util.Utils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
 
 

/**
 *
 * @author kevin
 */
public class HttpClientControl {
    
    private static HttpClientControl controller = null;
    
    private HttpClientControl(){
        getClient();
    }
    
    public static HttpClientControl getInstance(){
        if(controller == null){
            controller = new HttpClientControl();
        }
        
        return controller;
    }
      
    
    private static HttpClient client = null;
      
      private static HttpClient getClient(){
          if(client == null){
               PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
              client = new DefaultHttpClient(cm);
          }
          return client;
      }
      
      public String get(String path) {
        String result = "";
        try {
            HttpGet get = new HttpGet(path);
            HttpResponse response = client.execute(get);
            result = IOUtils.toString(response.getEntity().getContent());
        } catch (Exception e) {
            result = null;
            e.printStackTrace();
        }

        return result;
    }

    public String post(String path, String data) {
        String result = "";
        try {
            HttpPost request = new HttpPost(path);
            StringEntity params = new StringEntity(data);
            request.addHeader("content-type", "application/json");
            request.setEntity(params);
            HttpResponse response = client.execute(request);
            result = Utils.streamToString(response.getEntity().getContent());
        } catch (Exception e) {

        }
        return result;
    }

    public String put(String path, String data) {
        String result = "";
        try {
            HttpPut request = new HttpPut(path);
            StringEntity params = new StringEntity(data, "UTF-8");
            params.setContentType("application/json");
            request.addHeader("content-type", "application/json");
            request.addHeader("Accept", "*/*");
            request.addHeader("Accept-Encoding", "gzip,deflate,sdch");
            request.addHeader("Accept-Language", "en-US,en;q=0.8");
            request.setEntity(params);

            HttpResponse response = client.execute(request);
            result = Utils.streamToString(response.getEntity().getContent());
        } catch (Exception e) {

        }
        return result;
    }

    public String delete(String path, String data) {
        String result = "";
        try {
            HttpDelete request = new HttpDelete(path);

            HttpResponse response = client.execute(request);
            result = Utils.streamToString(response.getEntity().getContent());
        } catch (Exception e) {

        }
        return result;
    }
}
