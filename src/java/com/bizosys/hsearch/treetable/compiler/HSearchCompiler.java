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
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.treetable.CellGenerator;
import com.bizosys.hsearch.treetable.compiler.Schema.Column;
import com.bizosys.hsearch.treetable.compiler.Schema.Field;
import com.google.gson.Gson;

public class HSearchCompiler {
	public static void main(String[] args) throws Exception{
		
		Gson gson = new Gson();

		String schemaStr = fileToString(args[0]);
		//System.out.println(schemaStr);

		Schema newSchema = gson.fromJson(schemaStr, Schema.class);

		for (Column col : newSchema.columns) {
			List<Field> allFields = new ArrayList<Schema.Field>();
			allFields.addAll(col.indexes);
			allFields.add (col.key );
			allFields.add(col.value);
			
			FileWriterUtil.downloadToFile(generateHSearchTable(newSchema.module, col.key, col.value, col.name, allFields).getBytes(), 
					new File(args[1] + "/HSearchTable" + col.name + ".java") );

			FileWriterUtil.downloadToFile(generateHSearchPlugin(newSchema.module, col.name , col.key, col.value, allFields).getBytes(), 
					new File(args[1] + "/HSearchPlugin" + col.name + ".java") );
			
			FileWriterUtil.downloadToFile(generateHSearchTableCombinerImpl(newSchema).getBytes(), 
					new File(args[1] + "/HSearchTableCombinerImpl.java") );
			
			FileWriterUtil.downloadToFile(generateHSearchTableMultiQueryProcessorImpl(newSchema.module, col.key, col.value, allFields).getBytes(), 
					new File(args[1] + "/HSearchTableMultiQueryProcessorImpl.java") );
						
			FileWriterUtil.downloadToFile(generateHBaseCoprocessorAggregator(newSchema.module, col.key, col.value, allFields).getBytes(), 
					new File(args[1] + "/HBaseCoprocessorAggregator.java") );

			FileWriterUtil.downloadToFile(generateHBaseHSearchFilter(newSchema).getBytes(), 
					new File(args[1] + "/HBaseHSearchFilter.java") );

			FileWriterUtil.downloadToFile(generateHBaseTableReader(newSchema.module, col.key, col.value, allFields).getBytes(), 
					new File(args[1] + "/HBaseTableReader.java") );

			FileWriterUtil.downloadToFile(generateHBaseTableSchema(newSchema).getBytes(), 
					new File(args[1] + "/HBaseTableSchema.java") );

		}


		
		//System.out.println(template);

	}
	
	public static String generateHSearchTable(String module, Field fldKey, Field fldValue,
			String colName, List<Field> allFields) throws Exception {
		String template = fileToString("com/bizosys/hsearch/treetable/compiler/templates/HSearchTable.tmp");
		template = template.replace("--PACKAGE--", module);
		
		
		//Required Cells
		String reqCells = "";
		for ( int i=2; i<=allFields.size(); i++) {
			reqCells = reqCells + "import com.bizosys.hsearch.treetable.Cell" + i + ";\n";
		}
		template = template.replace("--IMPORT-CELLS--", reqCells);
		
		CodePartGenerator cg = new CodePartGenerator();
		
			
		template = template.replace("--DEFINE-EXACT--", cg.generatematchingCell(allFields, 1));
		template = template.replace("--DEFINE-MIN--", cg.generatematchingCell(allFields, 2));
		template = template.replace("--DEFINE-MAX--", cg.generatematchingCell(allFields, 3));
		template = template.replace("--DEFINE-MAPS--" , cg.generateMapContainers(allFields));

		template = template.replace("--DEFINE-LIST--" , cg.generateListContainers(allFields));
		
		Integer CELLMAX = allFields.size();
		Integer CELLMAX_MINUS_1 = CELLMAX - 1;
		String CELLMAX_MINUS_SUIGN = CodePartGenerator.cellSignatures.get(CELLMAX_MINUS_1.toString()) ;
		String CELLMAX_SIGN =  CELLMAX_MINUS_SUIGN.replace(  (CELLMAX_MINUS_1 + "<") , 
			(CELLMAX + "<" + allFields.get(0).datatype + ","));
		CELLMAX_SIGN =  CELLMAX_SIGN.replaceFirst(allFields.get(0).datatype + ", ", ""); 
		
		CodePartGenerator.cellSignatures.put(CELLMAX.toString(), CELLMAX_SIGN);
		
		template = template.replace("--CELLMAX-SIGN--",  CELLMAX_SIGN);
		template = template.replace("--CELL-SORTERS--",  cg.generateSorters(allFields) );
		
		template = template.replace("--VAL-COMPARATOR--",  cg.generateComparator(fldValue) );
		
		
		template = template.replace("--PUT-PARAMS-SIGNS--",  cg.generatePutParamsSigns(allFields));
		template = template.replace("--PUT-PARAMS--",  cg.generatePutParams(allFields));
		
		template = template.replace("--CELL-DATA--TYPES--", cg.generateParamTypes(allFields) );
		
		template = template.replace("--VAL-DATATYPE--", fldValue.datatype);
		
		//template = 	template.replace("--TREE-WALK-ROOT--", cg.createIterator(allFields, 1));
		String leafItrprefix = "";
		String leaftItrSuffix = "";
		for ( int i=1; i < (CELLMAX - 2) ; i++) {
			leafItrprefix = leafItrprefix + "while ( cell" + (CELLMAX - i ) + "Itr.hasNext() ) {\n";
			leafItrprefix = leafItrprefix + cg.createIterator(allFields, i ) ; 
			leaftItrSuffix = leaftItrSuffix + "}\n";
			
		}
		template = 	template.replace("--TREE-BROWSE-LEAF-PREFIX--", leafItrprefix );
		template = 	template.replace("--TREE-BROWSE-LEAF-SUFFIX--", leaftItrSuffix );
		
		template = 	template.replace("--CELL-MAX-MINUS-1--", CELLMAX_MINUS_1.toString());
		template = 	template.replace("--CELL-MAX-MINUS-1-SIGN--", CELLMAX_MINUS_SUIGN);
		
		String keyDataType = fldKey.datatype;
		String valDataType = fldValue.datatype;
		String valParentDataType = allFields.get( allFields.size() - 3).datatype;
		if ( "Short".equals(keyDataType) ) keyDataType = "Integer";
		if ( "Short".equals(valDataType) ) valDataType = "Integer";
		if ( "Short".equals(valParentDataType) ) valParentDataType = "Integer";

		template = 	template.replace("--KEY_DATATYPE--", keyDataType);
		template = 	template.replace("--VAL_DATATYPE--", valDataType);
		template = 	template.replace("--VAL-PARENT-DATATYPE--", valParentDataType);
		
		template = 	template.replace("--CELL-MAX-MINUS-2--", new Integer(CELLMAX - 2).toString());
		
		String keys = "";
		boolean isFirstTime = true;
		for ( int i=CELLMAX_MINUS_1; i>=3; i--) {
			if ( isFirstTime ) isFirstTime = false;
			else keys = keys + ", ";
			keys = keys + "cell" + i + "Key";
		}
		template = 	template.replace("--TREE-NODE-KEYS--", keys);
		
		//System.out.println( cg.createIterator(allFields, allFields.size() - 3)  );
		String listItrprefix = "";
		String listItrSuffix = "";
		for ( int i=1; i < (CELLMAX - 2) ; i++) {
			listItrprefix = listItrprefix + cg.createListIterator(allFields, i ) ; 
			listItrSuffix = listItrSuffix + "}\n";
			
		}
		template = 	template.replace("--LIST-BROWSE-LEAF-PREFIX--", listItrprefix );
		template = 	template.replace("--LIST-BROWSE-LEAF-SUFFIX--", listItrSuffix );
		
		template = 	template.replace("--COLUMN-NAME--", colName );
		
		return template;
	}
	
	public static String generateHSearchPlugin(String module, String colName, Field key, Field val, List<Field> allFields) throws Exception {
		String template = fileToString("com/bizosys/hsearch/treetable/compiler/templates/HSearchPlugin.tmp");

		template = template.replace("--PACKAGE--", module);
		
		String keyDataType = key.datatype;
		String valDataType = val.datatype;
		if ( "Short".equals(keyDataType) ) keyDataType = "Integer";
		if ( "Short".equals(valDataType) ) valDataType = "Integer";
		
		template = 	template.replace("--KEY_DATATYPE--", keyDataType);
		template = 	template.replace("--VAL_DATATYPE--", valDataType);
		template = 	template.replace("--CELL-MAX-MINUS-1--", new Integer(allFields.size() - 1).toString());
		
		template = 	template.replace("--ALL-COLS--", new CodePartGenerator().generateParamSign(allFields) );
		template = 	template.replace("--COLUMN-NAME--", colName );
		return template;
	}	
	
	public static String generateHSearchTableCombinerImpl(Schema schema) throws Exception {
		String template = fileToString("com/bizosys/hsearch/treetable/compiler/templates/HSearchTableCombinerImpl.tmp");
		template = template.replace("--PACKAGE--", schema.module);
		
		StringBuilder tables = new StringBuilder(4096);
		for ( Column column : schema.columns ) {
			tables.append("\t\tif ( tableType.equals(\"").append(column.name).append("\")) return new HSearchTable");
			tables.append(column.name).append("();\n");
		}

		template = template.replace("--CREATE-TABLES--", tables.toString());
		return template;
	}
	
	public static String generateHSearchTableMultiQueryProcessorImpl(String module, Field key, Field val,
			List<Field> allFields) throws Exception {
		String template = fileToString("com/bizosys/hsearch/treetable/compiler/templates/HSearchTableMultiQueryProcessorImpl.tmp");
		template = template.replace("--PACKAGE--", module);
		return template;
	}
	
	public static String generateHBaseCoprocessorAggregator(String module, Field key, Field val,
			List<Field> allFields) throws Exception {
		String template = fileToString("com/bizosys/hsearch/treetable/compiler/templates/HBaseCoprocessorAggregator.tmp");
		template = template.replace("--PACKAGE--", module);
		return template;
	}
	
	public static String generateHBaseTableReader(String module, Field key, Field val,
			List<Field> allFields) throws Exception {
		String template = fileToString("com/bizosys/hsearch/treetable/compiler/templates/HBaseTableReader.tmp");
		template = template.replace("--PACKAGE--", module);
		return template;
	}
	
	public static String generateHBaseTableSchema(Schema schema) throws Exception {
		String template = fileToString("com/bizosys/hsearch/treetable/compiler/templates/HBaseTableSchema.tmp");
		template = template.replace("--PACKAGE--", schema.module);
		
		StringBuilder families = new StringBuilder(4096);
		for ( Column column : schema.columns ) {
			
			families.append("List<String> ").append(column.name).append(" = new ArrayList<String>();\n");
			families.append("\t\tStringTokenizer token = new StringTokenizer(\"").append(column.partitions.values).append("\",\",\");\n");
			
			families.append("\t\twhile ( token.hasMoreTokens()) {\n");
			families.append("\t\t\t").append(column.name).append(".add(token.nextToken()); \n");
			families.append("\t\t}\n");
			families.append("\t\tHBaseTableSchemaDefn.getInstance().familyNames.put(\"").append(
				column.name).append("\", ").append(column.name).append(");\n");	
		}

		template = template.replace("--CREATE-COL-FAMILIES--", families.toString());
		return template;
	}
	
	public static String generateHBaseHSearchFilter(Schema schema) throws Exception {
		String template = fileToString("com/bizosys/hsearch/treetable/compiler/templates/HBaseHSearchFilter.tmp");
		template = template.replace("--PACKAGE--", schema.module);
		
		StringBuilder plugins = new StringBuilder(4096);
		boolean isFirst = true;
		for ( Column column : schema.columns ) {
			
			if ( isFirst ) isFirst = false;
			else plugins.append("else ");
			
			plugins.append("if ( type == \"").append(column.name).append("\") {\n");
			plugins.append("\treturn new HSearchPlugin").append(column.name).append("();\n");
			plugins.append("}\n");
		}
		
		template = template.replace("--CREATE-PLUGINS--", plugins.toString());
		
		
		return template;
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
