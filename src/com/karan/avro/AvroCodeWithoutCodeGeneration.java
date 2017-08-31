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
import org.json.simple.JSONObject;


//reference : https://dzone.com/articles/getting-started-avro-part-2

public class AvroCodeWithoutCodeGeneration {

	public static void main(String args[]) throws JsonParseException, JsonProcessingException, IOException{
		AvroCodeWithoutCodeGeneration AvroEx = new AvroCodeWithoutCodeGeneration();
		//AvroEx.serialize();
		AvroEx.deserialize();
	}
	
	
public void serialize() throws JsonParseException, JsonProcessingException, IOException{
		
		System.out.println(".. In AvroCodeWithoutCodeGeneration ..");
		java.io.InputStream in = new FileInputStream("/Users/karanalang/Documents/Technology/ApacheAvro/StudentActivity.json");
		
		//create a schema
		org.apache.avro.Schema schema = new Schema.Parser().parse(new File("/Users/karanalang/Documents/Technology/ApacheAvro/StudentActivity.avsc"));

		//create a Generic Record to hold json
		org.apache.avro.generic.GenericRecord AvroRec = new GenericData.Record(schema) ;
		//create a record to hold course data
		org.apache.avro.generic.GenericRecord CourseRec = new org.apache.avro.generic.GenericData.Record(schema.getField("course_details").schema());
		java.io.File AvroFile = new File("/Users/karanalang/Documents/Technology/ApacheAvro/StudentActivity1.avro");
		
		//create writer to serialize the record
		org.apache.avro.io.DatumWriter<GenericRecord> datumWriter = new org.apache.avro.generic.GenericDatumWriter<GenericRecord>(schema);
		org.apache.avro.file.DataFileWriter<GenericRecord> dataFileWriter =  new DataFileWriter<GenericRecord>(datumWriter);
		
		dataFileWriter.create(schema, AvroFile);
		
		//iterate over JSON present in input file, and write to Avro file
		for(Iterator it = new org.codehaus.jackson.map.ObjectMapper().readValues(new JsonFactory().createJsonParser(in), JSONObject.class); it.hasNext();) {
			org.json.simple.JSONObject JsonRec = (JSONObject) it.next();
			AvroRec.put("id", JsonRec.get("id"));
			AvroRec.put("student_id", JsonRec.get("student_id"));
			AvroRec.put("university_id", JsonRec.get("university_id"));
			
			LinkedHashMap CourseDetails = (LinkedHashMap) JsonRec.get("course_details");
			CourseRec.put("course_id", CourseDetails.get("course_id"));
			CourseRec.put("enroll_date", CourseDetails.get("enroll_date"));
			CourseRec.put("verb", CourseDetails.get("verb"));
			CourseRec.put("result_score", CourseDetails.get("result_score"));
		
			AvroRec.put("course_details", CourseRec);
			
			dataFileWriter.append(AvroRec);
		} //end of for loop
		
		in.close();
		dataFileWriter.close();

	} //end of serialize method

	
	public void deserialize() throws IOException{
		
		//create a schema 
		Schema schema = new Schema.Parser().parse(new File("/Users/karanalang/Documents/Technology/ApacheAvro/StudentActivity.avsc"));
		//create a record using schema
		GenericRecord AvroRec = new GenericData.Record(schema);
		java.io.File AvroFile = new File("/Users/karanalang/Documents/Technology/ApacheAvro/StudentActivity1.avro");
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(AvroFile, datumReader);
		
		System.out.println("Deserialized data is  : ");
	
		while (dataFileReader.hasNext()){
			AvroRec = dataFileReader.next(AvroRec);
			System.out.println(AvroRec);
		}	
	}
}


