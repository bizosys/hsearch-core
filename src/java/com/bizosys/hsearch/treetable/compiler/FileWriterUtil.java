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

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class FileWriterUtil {

	public static void replaceFile(InputStream is, File file, String from , String to ) {
		try {
			DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(file)));

			int chunk = 1024;
			byte[] bytes = new byte[chunk];
			boolean isAvailable = true;

			while(isAvailable) {
				int packet = is.read(bytes,0,chunk);
				if ( -1 == packet ) isAvailable  = false; 
				else {
					String strLine = new String(bytes);
					byte[] replacedBytes = strLine.replace(from, to).getBytes();
					out.write(replacedBytes,0,replacedBytes.length);
				}
			}
			out.flush();
			out.close();
			if ( is.markSupported() ) is.reset();
			System.out.println(file.getAbsolutePath() + "  is written sucessfully.");
			
		} catch(IOException e) {
			e.printStackTrace(System.err);
		} 
	}
	
	
	public static void downloadToFile(InputStream is, File file) {
		try {
			DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(file)));

			int chunk = 1024;
			byte[] bytes = new byte[chunk];
			boolean isAvailable = true;

			while(isAvailable) {
				int packet = is.read(bytes,0,chunk);
				if ( -1 == packet ) isAvailable  = false; 
				else out.write(bytes,0,packet);
			}
			out.flush();
			out.close();
			if ( is.markSupported() ) is.reset();
			System.out.println(file.getAbsolutePath() + "  is written sucessfully.");
			
		} catch(IOException e) {
			e.printStackTrace(System.err);
		} 
	}
	
	public static void downloadToFile(byte[] bytes, File file) {
		try {
			DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(file)));
			out.write(bytes);
			out.flush();
			out.close();
			System.out.println(file.getAbsolutePath() + "  is written sucessfully.");
			
		} catch(IOException e) {
			e.printStackTrace(System.err);
		} 
	}
	
	public static void downloadToFile(List<byte[]> bytes, File file) {
		try {
			DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(file)));
			for (byte[] bs : bytes) {
				out.write(bs);
			}
			out.flush();
			out.close();
			System.out.println(file.getAbsolutePath() + "  is written sucessfully.");
			
		} catch(IOException e) {
			e.printStackTrace(System.err);
		} 
	}
	
	/**
	 * Append the information to the file
	 * @param fileName
	 * @param text
	 */
	public static void appendToFile(String fileName, String text) {
	    try{
	    	FileWriter fstream = new FileWriter(fileName,true);
	    	BufferedWriter out = new BufferedWriter(fstream);
	        out.write(text);
	        out.close();
        } catch (Exception ex) {
        	ex.printStackTrace(System.err);
        }
	}
}