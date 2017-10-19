/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package club.capture.dbase.solr;

 
import club.capture.dbase.util.Log;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

/**
 *
 * @author kevin
 */
public class SolrServer {

    //server
    public static String ZOOKEEPER_URL = new String("localhost:9983");
    
    // collecdtions
    public static String DEFAULT_COLLECTION = new String("hud");
 
    
    private static final HashMap<String,CloudSolrClient> SOLR_CLIENTS = new HashMap<>();
    
    public static enum COLLECTIONS {
        default_collection(DEFAULT_COLLECTION);
        
        private String key = "";
        COLLECTIONS(String key){
            this.key = key;
        }
        
        public String key(){
            return key;
        }
    }
    
    public static synchronized void commit(ArrayList<SolrInputDocument> docs, COLLECTIONS collection) {
        SolrClient client = getSolrClient(collection.key);
        try {
            client.add(docs);
            client.commit(collection.key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
     public static synchronized void commit(ArrayList<SolrInputDocument> docs, String collection) {
        SolrClient client = getSolrClient(collection);
        try {
            client.add(docs);
            client.commit(collection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<SolrDocument> getAllTags() {
        SolrClient client = getSolrClient();
        ArrayList<SolrDocument> docs = new ArrayList<>();
        try {
            SolrQuery query = new SolrQuery();
            query.setQuery("tags:*");
            query.setFields("id","Agency","Office","Location","Title", "body", "tags");
            query.setFilterQueries("ResponseByTS:[NOW TO *]");
            QueryResponse resp = client.query(query);
            SolrDocumentList list = resp.getResults();
            for (int i = 0; i < list.size(); i++) {
                docs.add(list.get(i));
                System.out.println(list.get(i).getFirstValue("id"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return docs;
    }

    public static QueryResponse search(SolrQuery query) {
        QueryResponse resp = null;
        CloudSolrClient client = getSolrClient();
        try {
            resp = client.query(query);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resp;
    }

    private static CloudSolrClient getSolrClient() {
        CloudSolrClient cloudServer = null;

        String server = ZOOKEEPER_URL;
        String collection = DEFAULT_COLLECTION;
        try {
            PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
            HttpClient client = new DefaultHttpClient(cm);
            cloudServer = new CloudSolrClient(server, client);
            cloudServer.setDefaultCollection(collection);
            Log.log("CLOUD SERVER INIT OK...");
            
            

            //   cloudServer.add(docList);
            //    res = cloudServer.commit();
            //   System.out.println(res);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return cloudServer;
    }
    
        public static CloudSolrClient getSolrClient(String collection) {
        CloudSolrClient cloudServer = null;

        String server = ZOOKEEPER_URL;
 
        try {
            PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
            HttpClient client = new DefaultHttpClient(cm);
            cloudServer = new CloudSolrClient(server, client);
            cloudServer.setDefaultCollection(collection);
            Log.log("CLOUD SERVER INIT OK...");

            //   cloudServer.add(docList);
            //    res = cloudServer.commit();
            //   System.out.println(res);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return cloudServer;
    }



    public static CloudSolrClient getSolrClient(String server, String collection) {
        CloudSolrClient cloudServer = null;

        try {
            PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
            HttpClient client = new DefaultHttpClient(cm);
            cloudServer = new CloudSolrClient(server, client);
            cloudServer.setDefaultCollection(collection);
            Log.log("CLOUD SERVER INIT OK...collection: "+collection+" server: "+server);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return cloudServer;
    }
}
