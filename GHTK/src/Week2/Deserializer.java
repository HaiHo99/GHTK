package Week2;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class Deserializer {
	public static void main(String[] args) throws IOException {
		Schema schema = new Schema.Parser().parse(new File("‪C:\\Users\\Hai\\Desktop\\haiht34\\emp.avsc"));
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
	      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("C:\\Users\\Hai\\Desktop\\mydata.txt"), datumReader);
	      GenericRecord emp = null;
			
	      while (dataFileReader.hasNext()) {
	         emp = dataFileReader.next(emp);
	         System.out.println(emp);
	      }
	}
}
