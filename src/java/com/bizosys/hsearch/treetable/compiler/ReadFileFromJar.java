/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

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
