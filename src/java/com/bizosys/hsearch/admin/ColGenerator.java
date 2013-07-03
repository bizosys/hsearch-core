package com.bizosys.hsearch.admin;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.treetable.compiler.FileWriterUtil;

public class ColGenerator {

	static Map<String, Character> dataTypesPrimitives = new HashMap<String, Character>();
	
	static {
		dataTypesPrimitives.put("string", 't');
		dataTypesPrimitives.put("int", 'i');
		dataTypesPrimitives.put("float", 'f');
		dataTypesPrimitives.put("double", 'd');
		dataTypesPrimitives.put("long", 'l');
		dataTypesPrimitives.put("short", 's');
		dataTypesPrimitives.put("boolean", 'b');
		dataTypesPrimitives.put("byte", 'c');
	}

	public static void generate(FieldMapping fm , String path) throws IOException {

		String template = getTextFileContent("Column.tmp");
				
		String params = "";
		String setters = "";
		String getters = "";
		String stringSequencer = "";
		String intSequencer = "";
		String floatSequencer = "";
		String doubleSequencer = "";
		String longSequencer = "";
		String byteSequencer = "";
		String booleanSequencer = "";
		String shortSequencer = "";
		String setId = "";
		
		for (Map.Entry<String, Field> entry : fm.nameSeqs.entrySet()) {
			FieldMapping.Field fld = entry.getValue();
						
			String casted ="";
			String fieldValue = "";
			char dataTypeChar = dataTypesPrimitives.get(fld.fieldType.toLowerCase());

			switch (dataTypeChar) {
				case 't':
				{
					fieldValue = " = null";
					casted = "value.toString()";		
					stringSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.sourceName.toLowerCase() +";\n";				
				}
				break;
				case 'i':
				{
					fieldValue = " = 0";
					casted = "(Integer)value";
					intSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.sourceName.toLowerCase() +";\n";								
				}
				break;
				case 'f':
				{
					fieldValue = " = 0.0f";
					casted = "(Float)value";
					floatSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.sourceName.toLowerCase() +";\n";				
				}
				break;
				case 'd':
				{
					fieldValue = " = 0.0";
					casted = "(Double)value";
					doubleSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.sourceName.toLowerCase() +";\n";								
				}
				break;
				case 'l':
				{
					fieldValue = " = 0L";
					casted = "(Long)value";
					longSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.sourceName.toLowerCase() +";\n";				
				}
				break;
				case 's':
				{
					fieldValue = " = 0";
					casted = "(Short)value";
					shortSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.sourceName.toLowerCase() +";\n";				
				}
				break;
				case 'b':
				{
					fieldValue = " = false";
					casted = "(Boolean)value";
					booleanSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.sourceName.toLowerCase() +";\n";				
				}
				break;
				case 'c':
				{
					fieldValue = " = 0";
					casted = "(Byte)value";
					byteSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.sourceName.toLowerCase() +";\n";
				}
				break;
			}

			params += "\tpublic " + fld.fieldType + " " + fld.sourceName.toLowerCase() + fieldValue +";\n";
			setters += "\t\tcase "+ fld.sourceSeq + ":\n\t\t\t this."+fld.sourceName.toLowerCase()+" = " + casted + ";\n\t\t break;\n";
			getters += "\t\tcase "+ fld.sourceSeq + ":\n\t\t\t return this."+fld.sourceName.toLowerCase()+";\n";
			
			if(fld.isJoinKey){
				setId = "this." + fld.sourceName;
			}
		}

		template = template.replace("--PARAMS--", params);
		template = template.replace("--SETTERS--", setters);
		template = template.replace("--GETTERS--", getters);
		template = template.replace("--INTEGER_SEQUENCER--", intSequencer);
		template = template.replace("--FLOAT_SEQUENCER--", floatSequencer);
		template = template.replace("--STRING_SEQUENCER--", stringSequencer);
		template = template.replace("--DOUBLE_SEQUENCER--", doubleSequencer);
		template = template.replace("--LONG_SEQUENCER--", longSequencer);
		template = template.replace("--BOOLEAN_SEQUENCER--", booleanSequencer);
		template = template.replace("--SHORT_SEQUENCER--", shortSequencer);
		template = template.replace("--BYTE_SEQUENCER--", byteSequencer);
		template = template.replace("--SET_ID--", setId);
		
		//System.out.println(template);
		FileWriterUtil.downloadToFile(template.getBytes(),new File(path + "Column.java") );
	}

	public static String getTextFileContent(String fileName) throws IOException {
		InputStream stream = null; 
		Reader reader = null; 
		
		try {
			stream = ColGenerator.class.getResourceAsStream(fileName);
			
			reader = new BufferedReader ( new InputStreamReader (stream) );

	        byte[] bytes = new byte[1024]; // Create the byte array to hold the data
	        int numRead = 0;
	        
	        StringBuilder sb = new StringBuilder();
	        while (true) {
	        	numRead = stream.read(bytes, 0, 1024);
	        	if ( numRead == -1 ) break;
	        	
	        	sb.append(new String(bytes, 0, numRead));
	        }
	        
	        return sb.toString();
	        
		} finally {
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {ex.printStackTrace(System.err);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {ex.printStackTrace(System.err);}
		}
	}

	public static void main(String[] args) throws IOException {
	}

}
