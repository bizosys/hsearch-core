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

package com.bizosys.hsearch.treetable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;

public class CellGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		int cells = 13;
		String template = fileToString("com\\bizosys\\hsearch\\treetable\\CellN.txt");
		template = replaceClassNMinus1(template, cells);
		template = replaceClass(template, cells);
		template = replaceCellN(template, cells);
		template = replaceSortedDecl(template, cells);
		template = replaceSortedAssigner(template, cells);
		template = replaceSorters(template, cells);
		template = replaceSorterInstMinus1(template, cells);
		template = replaceSorterInst(template, cells);
		template = replaceValMinus1(template, cells);
		template = replaceLastArg(template, cells);
		template = cellMinus1(template, cells);
		template = replaceParamsN(template, cells);
		
		System.out.println(template);
		
	}
	
	public static String replaceClass(String clazzText, int size) {
		return clazzText.replaceAll("--CLASS--", "Cell" + size);
	}
	
	public static String replaceClassNMinus1(String clazzText, int size) {
		
		String cell = "Cell" + (size-1) + "<" ;
		
		for ( int i=2; i<size; i++) {
			if ( i != 2 ) cell = cell + ',';
			cell = cell + " K" + i;
		}	
		cell = cell + ",V>";
		return clazzText.replaceAll("--CELLN-1--", cell);
		
	}
	
	public static String replaceCellN(String clazzText, int size) {
		
		String cell = "Cell" + (size) + "<" ;
		
		for ( int i=1; i<size; i++) {
			if ( i != 1 ) cell = cell + ',';
			cell = cell + " K" + i;
		}	
		cell = cell + ",V>";
		return clazzText.replaceAll("--CELLN--", cell);
		
	}	
	
	public static String replaceSortedDecl(String clazzText, int size) {
		String decl = "";
		
		for ( int i=2; i<size; i++) {
			decl = decl + "public ISortedByte<K" + i + "> k" + i + "Sorter = null;\n\t";
		}	
		return clazzText.replaceAll("--SORTER_DECL--", decl);
	}
	
	public static String replaceSortedAssigner(String clazzText, int size) {
		String decl = "";
		
		for ( int i=1; i<size; i++) {
			decl = decl + "this.k" + i + "Sorter = k" + i + "Sorter;\n\t\t";
		}	
		return clazzText.replaceAll("--SORTER_ASSIGNER--", decl);
	}	
	
	public static String replaceSorters(String clazzText, int size) {
		String decl = "";
		
		for ( int i=1; i<size; i++) {
			if ( i != 1) decl = decl + ",";
			decl = decl + "ISortedByte<K" + i + "> k" + i + "Sorter";
		}	
		return clazzText.replaceAll("--SORTERS--", decl);
	}	
	
	public static String replaceSorterInstMinus1(String clazzText, int size) {
		String decl = "";
		
		for ( int i=2; i<size; i++) {
			if ( i != 2) decl = decl + ",";
			decl = decl + "k" + i + "Sorter";
		}	
		return clazzText.replaceAll("--SORTERN-1_INST--", decl);
	}
	
	public static String replaceSorterInst(String clazzText, int size) {
		String decl = "";
		
		for ( int i=1; i<size; i++) {
			if ( i != 1) decl = decl + ",";
			decl = decl + "k" + i + "Sorter";
		}	
		return clazzText.replaceAll("--SORTER_INST--", decl);
	}
	
	public static String replaceValMinus1(String clazzText, int size) {
		String decl = "";
		
		for ( int i=2; i<size; i++) {
			if ( i != 2) decl = decl + ",";
			decl = decl + "k" + i;
		}	
		decl = decl + ", v";
		return clazzText.replaceAll("--VAL_N-1--", decl);
	}	
	
	public static String replaceLastArg(String clazzText, int size) {
		return clazzText.replaceAll("--LAST_ARG--", "K" + (size-1));
	}	

	public static String cellMinus1(String clazzText, int size) {
		return clazzText.replaceAll("--CellN-1--", "cell" + (size-1));
	}	
	
	public static String replaceParamsN(String clazzText, int size) {
		String decl = "";
		
		for ( int i=1; i<size; i++) {
			if ( i != 1) decl = decl + ",";
			decl = decl + ("final K" + i + " k" + i);
		}	
		decl = decl + ", final V v";
		return clazzText.replaceAll("--PARAM_N--", decl);
	}	
		
	
	public static String fileToString(String fileName) 
	{
		
		File aFile = getFile(fileName);
		BufferedReader reader = null;
		InputStream stream = null;
		StringBuilder sb = new StringBuilder();
		try {
			stream = new FileInputStream(aFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			String line = null;
			String newline = "\n";
			while((line=reader.readLine())!=null) {
				if (line.length() == 0) continue;
				sb.append(line).append(newline);	
			}
			return sb.toString();
		} 
		catch (Exception ex) 
		{
			throw new RuntimeException(ex);
		} 
		finally 
		{
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {ex.printStackTrace(System.err);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {ex.printStackTrace(System.err);}
		}
	}	
	
	   public static File getFile(String fileName) 
	    {
			File aFile = new File(fileName);
			if (aFile.exists()) return aFile;
			
			aFile = new File("/" + fileName);
			if (aFile.exists()) return aFile;

			aFile = new File("conf/" + fileName);
			if (aFile.exists()) return aFile;
			
			aFile = new File("resources/" + fileName);
			if (aFile.exists()) return aFile;
			
			try {
				URL resource = CellGenerator.class.getClassLoader().getResource(fileName);
				if ( resource != null) aFile = new File(resource.toURI());
			} 
			catch (URISyntaxException ex) {
				throw new RuntimeException(ex);
			}

			if (aFile.exists()) return aFile;

			throw new RuntimeException("FileResourceUtil > File does not exist :" + fileName);
		}
	    	

}