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

public class CodePartGenerator {
	
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

	public String generateMapContainers(List<Field> fields) throws Exception {

		int totalFields = fields.size();
		
		String allMaps = "";
		
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
			
			allMaps = allMaps + "\t\tMap<" + cellSign + "> cell" + cellNo + "L = new HashMap<" + cellSign + ">();\n"; 
		}
		return allMaps;
	}
	
	public String generateListContainers(List<Field> fields) throws Exception {

		int totalFields = fields.size();
		
		String allLists = "";
		
		int startIndex = 0;
		int limit = totalFields;
		int cellNo = limit; 
		for ( int i=startIndex; i<limit-2; i++ ) {
			String parentKeyDataType = fields.get(i).datatype;
			if ( parentKeyDataType.equals("Short")) parentKeyDataType = "Integer";
			
			cellNo--;
			String cellSign = "Cell" + cellNo + "<";
			boolean firstTime = true;
			for ( int j=i + 1; j<limit; j++ ) {
				if ( firstTime ) {
					firstTime = false;
				} else cellSign = cellSign + ", ";
				
				if ( fields.get(j).datatype.equals("Short")) cellSign = cellSign + "Integer";
				else cellSign = cellSign + fields.get(j).datatype;
			}
			cellSign = cellSign + ">";
			
			allLists = allLists + "\t\tList<" + cellSign + "> cell" + cellNo + "L = new ArrayList<" + cellSign + ">();\n"; 
		}
		return allLists;
	}

	public String generatePutParamsSigns(List<Field> fields) throws Exception {

		String allParams = "";
		
		boolean firstTime = true;
		for ( Field fld : fields) {
			if ( firstTime ) {
				firstTime = false;
			} else allParams = allParams + ", ";

			String name = fld.name;
			String dataType = fld.datatype;
			if ( dataType.equals("Short")) dataType = "Integer";
			
			allParams = allParams + dataType + " " + name;
			
		}
		return allParams;
	}
	
	public String generatePutParams(List<Field> fields) throws Exception {

		String allParams = "";
		
		boolean firstTime = true;
		for ( Field fld : fields) {
			if ( firstTime ) {
				firstTime = false;
			} else allParams = allParams + ", ";

			String name = fld.name;
			allParams = allParams + name;
			
		}
		return allParams;
	}	
	
	public String generateParamTypes(List<Field> fields) throws Exception {

		String allParams = "";
		
		boolean firstTime = true;
		for ( Field fld : fields) {
			if ( firstTime ) {
				firstTime = false;
			} else allParams = allParams + ", ";

			String type = fld.datatype;
			if ( "Short".equals(type)) type = "Integer";
			allParams = allParams + "\"" + type + "\"" ;
			
		}
		return allParams;
	}		
	
	public String generateParamSign(List<Field> fields) throws Exception {

		String allParams = "";
		
		boolean firstTime = true;
		int seq = 1;
		for ( Field fld : fields) {
			if ( firstTime ) {
				firstTime = false;
			} else allParams = allParams + ", ";

			String type = fld.datatype;
			if ( "Short".equals(type)) type = "Integer";
			allParams = allParams + type + " cell" + seq;
			seq++;
		}
		return allParams;
	}	

	
	public String generateComparator(Field valField) throws Exception {
		if ( "Integer".equals(valField.datatype) ) return "IntComparator";
		else if ("Double".equals(valField.datatype)) return "DoubleComparator";
		else if ("Float".equals(valField.datatype)) return "FloatComparator";
		else if ("Long".equals(valField.datatype)) return "LongComparator";
		else if ("String".equals(valField.datatype)) return "StringComparator";
		else if ("Byte".equals(valField.datatype)) return "ByteComparator";
		else if ("Short".equals(valField.datatype)) return "ShortComparator";
		else throw new Exception("Datatype is not found - " + valField.datatype );
	}
	
	
	public String generateSorters(List<Field> fields) throws Exception {

		String allSorters = "";
		
		for ( Field field :  fields) {
			if ( allSorters.length() > 0 ) allSorters = allSorters + ",\n\t\t\t\t";
			
			String dataType = field.datatype;
			double minVal = field.minValue;
			
			if ("Integer".equals(dataType)) allSorters = allSorters + "SortedBytesInteger.getInstance()";
			else if ("String".equals(dataType)) allSorters = allSorters + "SortedBytesString.getInstance()";
			else if ("Float".equals(dataType)) allSorters = allSorters + "SortedBytesFloat.getInstance()";
			else if ("Long".equals(dataType)) allSorters = allSorters + "SortedBytesLong.getInstance()";
			else if ("Double".equals(dataType)) allSorters = allSorters + "SortedBytesDouble.getInstance()";
			else if ("Byte".equals(dataType)) allSorters = allSorters + "SortedBytesChar.getInstance()";
			else if ("Boolean".equals(dataType)) allSorters = allSorters + "SortedBytesBoolean.getInstance()";
			else if ("Short".equals(dataType)) allSorters = allSorters + 
					"SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) " + minVal + " ) ";
			
		}
		return allSorters;
	}	
	
	public String createIterator(List<Field> fields, int cellNo) {
		
		int remainingCells = fields.size() - cellNo;
		int remainingCellsValueIndex = remainingCells - 1;
		String cellSignatureKey = fields.get(cellNo - 1).datatype;
		if ( cellSignatureKey.equals("Short")) cellSignatureKey = "Integer";
		String theValueCellSignature = cellSignatures.get(new Integer(remainingCells).toString());
		theValueCellSignature = theValueCellSignature.replace("Short", "Integer");
		
		String theRemainingValueCellSignature = cellSignatures.get(new Integer(remainingCellsValueIndex).toString());
		theRemainingValueCellSignature = theRemainingValueCellSignature.replace("Short", "Integer");
		
		
		String code = "Entry<" + theValueCellSignature + "> cell" +   
			remainingCells + " = cell" + remainingCells + "Itr.next();\n";
		code = code + cellSignatureKey + " cell" + remainingCells + "Key = cell" + remainingCells + ".getKey();\n";
		code = code + theValueCellSignature.replaceFirst(cellSignatureKey + ", ", "") + 
				" cell" + remainingCells + "Val = cell" + remainingCells + ".getValue();\n";

		code = code + "if ( query.filterCells[" + cellNo + "] ) {\n" +
			"\tcell" + remainingCells + "Val.getMap( matchingCell" + cellNo + ", cellMin" + cellNo + ", cellMax" + cellNo + ", cell" + remainingCellsValueIndex + "L);\n" +
			"} else {\n" + 
			"\tcell" + remainingCells + "Val.sortedList = cell" + (remainingCellsValueIndex) + "L;\n" + 
			"\tcell" + remainingCells + "Val.parseElements();\n" + 
			"}\n" + 
			"Iterator<Entry<" + theRemainingValueCellSignature + ">> cell" + 
			(remainingCellsValueIndex) + "Itr = cell" + (remainingCellsValueIndex) + "L.entrySet().iterator();\n" ;  
		
		return code;
	}

	public String createListIterator(List<Field> fields, int cellNo) {
		
		int remainingCells = fields.size() - cellNo;
		int remainingCellsValueIndex = remainingCells - 1;
		
		String cellSignatureKey = fields.get(cellNo - 1).datatype;
		if ( cellSignatureKey.equals("Short")) cellSignatureKey = "Integer";
		String theValueCellSignature = cellSignatures.get(new Integer(remainingCells).toString());
		theValueCellSignature = theValueCellSignature.replace("Short", "Integer");
		
		String theRemainingValueCellSignature = cellSignatures.get(new Integer(remainingCellsValueIndex).toString());
		theRemainingValueCellSignature = theRemainingValueCellSignature.replace("Short", "Integer");
		
		String currentCellSign = theValueCellSignature.replaceFirst(cellSignatureKey + ", ", "") ;

		String code="";
		code = code +"for("+currentCellSign+"cell" + remainingCells +" : cell"+remainingCells+"L){\n";	
		code = code + "\tcell"+remainingCellsValueIndex+"L.clear();\n";
		code = code +"\tif ( query.filterCells[" + cellNo + "] ) {\n"+
			"\t\tcell"+remainingCells+".valuesUnchecked(matchingCell"+cellNo+", cellMin"+cellNo+", cellMax"+cellNo+", cell"+remainingCellsValueIndex+"L);\n"+
		"\t} else {\n"+
			"\t\tcell"+remainingCells+".values(cell"+remainingCellsValueIndex+"L);\n}\n";
		
		return code;
	}
	
	public String generatematchingCell(List<Field> fields, int castType) throws Exception {
		try {
			int seq = -1;
			String text = "";
			for (Field field : fields) {
				seq++;
				char firstchar = dataTypes.get(field.datatype);
				switch ( firstchar) {
				  	case 'i':
				  		switch ( castType ) {
				  			case 1:
						  		text = text +  ("Integer matchingCell" + seq + " = ( query.filterCells[" + seq + 
						  				"] ) ? (Integer) query.exactValCellsO[" + seq + "]: null;\n");
						  		break;
				  			case 2:
						  		text = text +  ("Integer cellMin" + seq + " = ( query.minValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[" + seq + "]).intValue();\n");
						  		break;
				  			case 3:
						  		text = text +  ("Integer cellMax" + seq + " =  (query.maxValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[" + seq + "]).intValue();\n");
						  		break;
				  		}
				  		break;
				  	case 'f':
				  		switch ( castType ) {
				  			case 1:
						  		text = text +  ("Float matchingCell" + seq + " = ( query.filterCells[" + seq + 
						  				"] ) ? (Float) query.exactValCellsO[" + seq + "]: null;\n");
						  		break;
				  			case 2:
						  		text = text +  ("Float cellMin" + seq + " = ( query.minValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[" + seq + "]).floatValue();\n");
						  		break;
				  			case 3:
						  		text = text +  ("Float cellMax" + seq + " =  (query.maxValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[" + seq + "]).floatValue();\n");
						  		break;
				  		}
				  		break;
				  	case 'd':
				  		switch ( castType ) {
				  			case 1:
						  		text = text +  ("Double matchingCell" + seq + " = ( query.filterCells[" + seq + 
						  				"] ) ? (Double) query.exactValCellsO[" + seq + "]: null;\n");
						  		break;
				  			case 2:
						  		text = text +  ("Double cellMin" + seq + " = ( query.minValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : query.minValCells[" + seq + "];\n");
						  		break;
				  			case 3:
						  		text = text +  ("Double cellMax" + seq + " =  (query.maxValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : query.maxValCells[" + seq + "];\n");
						  		break;
				  		}
				  		break;
				  	case 'l':
				  		switch ( castType ) {
				  			case 1:
						  		text = text +  ("Long matchingCell" + seq + " = ( query.filterCells[" + seq + 
						  				"] ) ? (Long) query.exactValCellsO[" + seq + "]: null;\n");
						  		break;
				  			case 2:
						  		text = text +  ("Long cellMin" + seq + " = ( query.minValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[" + seq + "]).longValue();\n");
						  		break;
				  			case 3:
						  		text = text +  ("Long cellMax" + seq + " =  (query.maxValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[" + seq + "]).longValue();\n");
						  		break;
				  		}
				  		break;
				  	case 's':
				  		switch ( castType ) {
				  			case 1:
						  		text = text +  ("Integer matchingCell" + seq + " = ( query.filterCells[" + seq + 
						  				"] ) ? (Integer) query.exactValCellsO[" + seq + "]: null;\n");
						  		break;
				  			case 2:
						  		text = text +  ("Integer cellMin" + seq + " = ( query.minValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[" + seq + "]).intValue();\n");
						  		break;
				  			case 3:
						  		text = text +  ("Integer cellMax" + seq + " =  (query.maxValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[" + seq + "]).intValue();\n");
						  		break;
				  		}
				  		break;
				  	case 't':
				  		switch ( castType ) {
				  			case 1:
						  		text = text +  ("String matchingCell" + seq + " = ( query.filterCells[" + seq + 
						  				"] ) ? (String) query.exactValCellsO[" + seq + "]: null;\n");
						  		break;
				  			case 2:
						  		text = text +  ("String cellMin" + seq + " = null;\n");
						  		break;
				  			case 3:
						  		text = text +  ("String cellMax" + seq + " = null;\n");
						  		break;
				  		}
				  		break;
				  	case 'c':
				  		switch ( castType ) {
				  			case 1:
						  		text = text +  ("Byte matchingCell" + seq + " = ( query.filterCells[" + seq + 
						  				"] ) ? (Byte) query.exactValCellsO[" + seq + "]: null;\n");
						  		break;
				  			case 2:
						  		text = text +  ("Byte cellMin" + seq + " = ( query.minValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[" + seq + "]).byteValue();\n");
						  		break;
				  			case 3:
						  		text = text +  ("Byte cellMax" + seq + " =  (query.maxValCells[" + seq + 
						  				"] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[" + seq + "]).byteValue();\n");
						  		break;
				  		}
				  		break;
				  	case 'b':
				  		switch ( castType ) {
				  			case 1:
						  		text = text +  ("Boolean matchingCell" + seq + " = ( query.filterCells[" + seq + 
						  				"] ) ? (Boolean) query.exactValCellsO[" + seq + "]: null;\n");
						  		break;
				  			case 2:
						  		text = text +  ("Boolean cellMin" + seq + " = null;\n");
						  		break;
				  			case 3:
						  		text = text +  ("Boolean cellMax" + seq + " = null;\n");
						  		break;
				  		}
				  		break;
				  }
		  	  }
		  	return text;
		  } catch (Exception ex) {
			  ex.printStackTrace(System.err);
			  
			  throw ex;
		  }
	}
}