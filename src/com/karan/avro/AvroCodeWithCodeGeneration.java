package com.karan.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;	
import org.apache.avro.io.DatumWriter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
//import org.json.simple.JSONObject;



//reference : https://dzone.com/articles/getting-started-avro-part-2

public class AvroCodeWithCodeGeneration {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public void serialize() throws JsonParseException, JsonProcessingException, IOException{
		
		InputStream in = new FileInputStream("/Users/karanalang/Documents/Technology/ApacheAvro/StudentActivity.json");
		
		//create a schema
//		Schema schema = new Schema.Parser().parse(new File("/Users/karanalang/Documents/Technology/ApacheAvro/StudentActivity.avsc"));
//		StudentActivity sa = new StudentActivity();
		
	}
	
	public void deserialize() throws IOException{
		
	}
	
	
	
}
