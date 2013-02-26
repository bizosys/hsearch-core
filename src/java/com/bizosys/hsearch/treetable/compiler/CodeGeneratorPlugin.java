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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.treetable.compiler.Schema.Field;

public class CodeGeneratorPlugin {

	static Map<String, Character> dataTypes = new HashMap<String, Character>();
	static {
		dataTypes.put("Double", 'd');
		dataTypes.put("Long", 'l');
		dataTypes.put("Integer", 'i');
		dataTypes.put("Float", 'f');
		dataTypes.put("Short", 's');
		dataTypes.put("Boolean", 'b');
		dataTypes.put("String", 't');
		dataTypes.put("Byte", 'c');
	}

	public static Map<String, String> cellSignatures = new HashMap<String, String>();

	public String generateCellSignature(List<Field> fields,int cellNo) throws Exception {

		int remainingCells = fields.size() - cellNo;
		int remainingCellsValueIndex = remainingCells - 1;
		
		String theValueCellSignature = cellSignatures.get(new Integer(remainingCells).toString());
		if(theValueCellSignature == null){
			createCellSignatures(fields);
			theValueCellSignature = cellSignatures.get(new Integer(remainingCells).toString());
		}
		
		String cellSignatureKey = fields.get(cellNo - 1).datatype;
		if ( cellSignatureKey.equals("Short")) cellSignatureKey = "Integer";
		theValueCellSignature = theValueCellSignature.replace("Short", "Integer");
		
		String theRemainingValueCellSignature = cellSignatures.get(new Integer(remainingCellsValueIndex).toString());
		theRemainingValueCellSignature = theRemainingValueCellSignature.replace("Short", "Integer");
		
		String currentCellSign = theValueCellSignature.replaceFirst(cellSignatureKey + ", ", "") ;
		
		return currentCellSign;
	}
	
	public void createCellSignatures(List<Field> fields){

		int totalFields = fields.size();
		
		int startIndex = 0;
		int limit = totalFields;
		int cellNo = limit; 
		for ( int i=startIndex; i<limit-2; i++ ) {
			String parentKeyDataType = fields.get(i).datatype;
			if ( parentKeyDataType.equals("Short")) parentKeyDataType = "Integer";
			
			cellNo--;
			String cellSign = parentKeyDataType + ", Cell" + cellNo + "<";
			boolean firstTime = true;
			for ( int j=i + 1; j<limit; j++ ) {
				if ( firstTime ) {
					firstTime = false;
				} else cellSign = cellSign + ", ";
				
				if ( fields.get(j).datatype.equals("Short")) cellSign = cellSign + "Integer";
				else cellSign = cellSign + fields.get(j).datatype;
			}
			cellSign = cellSign + ">";
			cellSignatures.put(new Integer(cellNo).toString(), cellSign);
		}

	}
	
	public String generateOnRowParamsSigns(List<Field> fields) throws Exception {

		String allParams = "";
		String name = "";
		String dataType = "";
		int count = 0;
		boolean firstTime = true;
		for ( Field field : fields) {
			if ( firstTime ) {
				firstTime = false;
			} else allParams = allParams + ", ";

			name = "cell"+count;
			dataType = field.datatype;
			if ( dataType.equals("Short")) dataType = "Integer";
			
			allParams = allParams + dataType + " " + name;
			
			count++;
		}
		return allParams;
	}

}
