package io.confluent.connect.avro;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;

import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class utils {

    public Integer getCsrSchemaId(String subject, Map<Integer,Integer> schemaMap, Integer schemaVersionId, String hortonSRbaseURL, Boolean autoregister, SchemaRegistryClient schemaRegistry) throws IOException, RestClientException {
        //int cfltSchmaIdfromMap=0;
        int id=0;
        String schemaText=null;
        //System.out.println("map" +  schemaMap.isEmpty());
        if(!schemaMap.isEmpty() & !(schemaMap.get(schemaVersionId)==null))
            id=schemaMap.get(schemaVersionId);

        //for (Map.Entry<Integer,Integer> entry : schemaMap.entrySet())
            //System.out.println("Key = " + entry.getKey() +
                   // ", Value = " + entry.getValue());
        //System.out.println("CFLT Id" +  id);
       // System.out.println("auto register" +  autoregister);

        if(id == 0 & autoregister){
            id= registerSchemaCSR(subject,hortonSRbaseURL,schemaVersionId,schemaRegistry);
        }
        else if(!autoregister & id==0) {
            //Auto register false impl
            String subjecturl= hortonSRbaseURL+"versionsById/"+schemaVersionId;
           try{

               //TODO: Handle schema out of order registration , below line of code should work :
               /**
                ParsedSchema scm2=new AvroSchema(new schemaParser().parse(getSchemaTextHSR(subjecturl,false)));
                              id = schemaRegistry.getId(subject,scm2);
                **/
               id = schemaRegistry.getSchemaMetadata(subject,
                       Integer.valueOf(getSchemaTextHSR(subjecturl,true)),
                       false).getId();
           }
           catch (RestClientException e){
               if(e.getErrorCode() == 40401)
                   throw new RuntimeException(
                           String.format("Failed to fetch schema ID for  topic %s : Auto register should be configured to register schemas automatically or schemas must be present in registry for subject : ", subject)
                   );
               else
                   throw e;

            }
            if (id==0) {
                throw new RuntimeException(
                        String.format("Unexpected error occured , cannot replicate message")
                );
            }
        }

return id;
    }
    public Integer getHsrSchemId(Integer schemaId,String hsrScmVerPath,String subject,Map<String,Integer> schemaMap, String hortonSRbaseURL, Boolean autoregister, SchemaRegistryClient schemaRegistry) throws IOException, RestClientException {
        int id=0;
        if(!schemaMap.isEmpty() & !(schemaMap.get(subject+schemaId)==null))
            id=schemaMap.get(subject+schemaId);

        if (id==0 & autoregister){
                id=registerSchemaHSR(subject,hsrScmVerPath,schemaId,hortonSRbaseURL,schemaRegistry);

        }
        else if(!autoregister & id==0){
            //System.out.println(schemaRegistry.getVersion(subject+"-value",schemaRegistry.getSchemaById(schemaId)));
            String subjecturl = hortonSRbaseURL + subject + hsrScmVerPath +
                    schemaRegistry.getVersion(subject+"-value",schemaRegistry.getSchemaById(schemaId));;
            id=getSchemaVersionIdHSR(subjecturl,subject);

            if (id==0) {
                throw new RuntimeException(
                        String.format("Failed to register schema for  topic %s : Auto register should be configured to register schemas automatically or schemas must be present in registry for subject : ", subject)
                );
            }
        }
    return id;
    }
    public String getSchemaTextHSR(String subjecturl, Boolean getVersion) throws IOException {
    URL urlobj = new URL(subjecturl);
    HttpURLConnection conn = (HttpURLConnection) urlobj.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");
    BufferedReader br = new BufferedReader(new InputStreamReader(
            (conn.getInputStream())));
    String outputFinal;
    String schemaText="";
    String version="";
    while ((outputFinal = br.readLine()) != null) {
        JSONObject obj = new JSONObject(outputFinal);
        schemaText = (obj.get("schemaText").toString());
        version=(obj.get("version")).toString();

    }
    conn.disconnect();
    br.close();
    if(getVersion)
        return version;
    return schemaText;
}
    public Integer registerSchemaCSR(String subject,String hortonSRbaseURL, Integer schemaVersionId , SchemaRegistryClient schemaRegistry) throws IOException, RestClientException {
        //System.out.println("metadata null, registering");
        String subjecturl= hortonSRbaseURL+"versionsById/"+schemaVersionId;
        String schemaText=getSchemaTextHSR(subjecturl,false);
       // System.out.println("schema text" + schemaText);

        ParsedSchema scm2=new AvroSchema(new schemaParser().parse(schemaText));
        int id=schemaRegistry.register(subject,scm2);

        return id;

    }
    public void registerSchemaMetadataHSR(String hortonSRbaseURL,String subject) throws IOException {
    URL urlobjSchemas=new URL(hortonSRbaseURL);
    HttpURLConnection conn = (HttpURLConnection) urlobjSchemas.openConnection();
    JSONObject postreq=new JSONObject();
    conn.setDoOutput(true);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("accept", "application/json");
    conn.setRequestProperty("Content-Type", "application/json");
    postreq.put("name",subject);
    postreq.put("type","avro");
    postreq.put("schemaGroup","Kafka");
    postreq.put("evolve","true");
    postreq.put("compatibility","BACKWARD");
    postreq.put("description","This is a auto created schema by confluent replicator for topic-" + subject);
    //String postreqStr = postreq.toString();
    ////System.out.println(postreqStr);
    try (DataOutputStream dos = new DataOutputStream(conn.getOutputStream())) {
        dos.writeBytes(String.valueOf(postreq));
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(
            (conn.getInputStream())));
    String output;
    while ((output = br.readLine()) != null) {
        //System.out.println(output);
    }
    conn.disconnect();
    br.close();
}
    public String registerSchemaVersionHSR(String hortonSRbaseURL, String subject,String schemaText ) throws IOException {
    URL urlobj = new URL(hortonSRbaseURL + subject + "/versions?branch=MASTER");
    HttpURLConnection conn = (HttpURLConnection) urlobj.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("accept", "application/json");
    conn.setRequestProperty("Content-Type", "application/json");
    JSONObject postreqscmver=new JSONObject();
    postreqscmver.put("schemaText",schemaText);
    postreqscmver.put("description","This is a auto created schema version by confluent replicator for topic-" + subject);
    try (DataOutputStream dos = new DataOutputStream(conn.getOutputStream())) {
        dos.writeBytes(String.valueOf(postreqscmver));
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(
            (conn.getInputStream())));
    String outputVer;
    String hsrscm="";
    while ((outputVer = br.readLine()) != null) {
        hsrscm=outputVer.toString();
        //System.out.println("Created schema version"  + outputVer.toString());

    }
    conn.disconnect();
    br.close();
    return hsrscm;
}
    public Integer getSchemaVersionIdHSR(String subjecturl,String subject) throws IOException {
    URL urlobj = new URL(subjecturl);
    HttpURLConnection conn = (HttpURLConnection) urlobj.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");
    conn.connect();
    try{
        BufferedReader br = new BufferedReader(new InputStreamReader(
                (conn.getInputStream())));
        String outputFinal;
        int id=0;
        while ((outputFinal = br.readLine()) != null) {
            JSONObject obj = new JSONObject(outputFinal);
            id = Integer.valueOf(obj.get("id").toString());
            //System.out.println("Id obtained" +id);
        }
        br.close();

        conn.disconnect();
        return id;
    }
    catch(FileNotFoundException e){
        throw new RuntimeException(
                String.format("Failed to fetch schema ID for " +
                        " topic %s : Auto register should be configured to register " +
                        "schemas automatically or schemas must be present in registry for subject : ", subject)
        );
    }
}
    public Integer registerSchemaHSR(String subject, String hsrScmVerPath, Integer schemaId,String hortonSRbaseURL , SchemaRegistryClient schemaRegistry) throws IOException, RestClientException {
        int id=0;
        //first register the schema metadata
        registerSchemaMetadataHSR(hortonSRbaseURL,subject);

        //now register the schema version
        String schemaText=schemaRegistry.getSchemaById(schemaId).toString();
        String hsrscm= registerSchemaVersionHSR(hortonSRbaseURL,subject,schemaText);

        //System.out.println(hsrscm);
        //Now get the registered version because the previous one doesn't give the right ID
        String subjecturl = hortonSRbaseURL + subject + hsrScmVerPath + hsrscm;
        id=getSchemaVersionIdHSR(subjecturl,subject);
return id;
    }

}
