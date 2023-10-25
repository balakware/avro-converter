package io.confluent.connect.avro;
import org.apache.avro.Schema;
public class schemaParser {
    
    public Schema parse(String schemaText){
        Schema scm= new Schema.Parser().parse(schemaText.toString());
        return scm;
    }
}
