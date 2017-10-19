/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package club.capture.dbase;

import club.capture.dbase.solr.HttpClientControl;
import club.capture.dbase.solr.SolrServer;
import club.capture.dbase.util.Log;
import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFUtils;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.UUID;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 *
 * @author kevin
 */
public class DBaseReader {
    
    public final static HttpClientControl client = HttpClientControl.getInstance();

    public static void main(String args[]) {
        
        

        DBFReader reader = null;
        CloudSolrClient solr = SolrServer.getSolrClient("hud");
        try {

            // create a DBFReader object
            reader = new DBFReader(new FileInputStream("lihtcpub/LIHTCPUB.DBF"));
            LinkedHashMap<Integer,String> map = new LinkedHashMap<>();

            // get the field count if you want for some reasons like the following
            int numberOfFields = reader.getFieldCount();

            // use this count to fetch all field information
            // if required
            if (numberOfFields > 0) {
                Log.log("Create Headers...");
            }
            for (int i = 0; i < numberOfFields; i++) {

                DBFField field = reader.getField(i);

                // do something with it if you want
                // refer the JavaDoc API reference for more details
                //
                System.out.println(field.getName());
                /** @TODO: Need to add a check for field's existence */
                map.put(i,field.getName());
            //    addField(field.getName()); 

            }

            // Now, lets us start reading the rows
            Object[] rowObjects;

            Log.log("Read Rows...");
            ArrayList<SolrInputDocument> docs = new ArrayList<>();
            SolrInputDocument doc = null;
            while ((rowObjects = reader.nextRecord()) != null) {
                doc = new SolrInputDocument();
                doc.setField("id", UUID.randomUUID());
                for (int i = 0; i < rowObjects.length; i++) {
                    System.out.println(rowObjects[i]);
                    doc.setField(map.get(i), rowObjects[i]);
                }
                docs.add(doc);
            }
            
            
            // now commit in batches of 5k
            int count = 0; 
            int max = 5000;
            ArrayList<SolrInputDocument> tdocs = new ArrayList<>();
           
            for(int j=0; j<docs.size(); j++){
               doc = docs.get(j);
               tdocs.add(doc);
                count++;
                if(count >= max){
                    commit(tdocs);
                    tdocs = new ArrayList<>();
                    count = 0;
                }
            }
            
            if(tdocs.size() > 0){
                commit(tdocs);
            }

            // By now, we have iterated through all of the rows
        } catch (DBFException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            DBFUtils.close(reader);
        }
    }
    
    private static synchronized void commit(ArrayList<SolrInputDocument> docs){
         SolrServer.commit(docs, "hud");
    }

    private static void addField(String fieldName) {
        String json = "{\n"
                + "  \"add-field\":{\n"
                + "     \"name\":\"" + fieldName + "\",\n"
                + "     \"type\":\"string\",\n"
                + "     \"indexed\":\"true\",\n"
                + "     \"stored\":true }\n"
                + "}";
        
       String response = client.post("http://localhost:8983/solr/hud/schema", json);
       Log.log(response);

    }
}
