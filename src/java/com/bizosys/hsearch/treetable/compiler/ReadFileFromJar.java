package com.bizosys.hsearch.treetable.compiler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class ReadFileFromJar {

	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		System.out.println( 
			getTextFileContent("/com/bizosys/hsearch/treetable/compiler/schema-search.txt") );
	}
	
	public static String getTextFileContent(String fileName) throws IOException {
		InputStream stream = null; 
		Reader reader = null; 
		
		try {
			stream = ReadFileFromJar.class.getResourceAsStream(fileName);
			
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

}
