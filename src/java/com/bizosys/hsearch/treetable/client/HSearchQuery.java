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

package com.bizosys.hsearch.treetable.client;

import java.io.InvalidObjectException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.regex.Pattern;

import com.bizosys.hsearch.util.HSearchLog;

/**
 * 	@author abinash
 */
public class HSearchQuery {

	private static final String QUOTES = "\"";

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	
	private static final char RANGE_SEPARATOR = ':';
	private static final String FIELD_SEPARATOR = "|";
	private static final Pattern patternCommaOutsideQuotes = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	private static final String QUOTED_FIELD_SEPARATOR = "(?<!\\\\)" + Pattern.quote(FIELD_SEPARATOR);
	private static final Pattern PATTERN_PIPE_SPLITTER = Pattern.compile(QUOTED_FIELD_SEPARATOR);
	private static final Pattern PATTERN_ESCAPER = Pattern.compile("\\\\");
	
	
	public static double DOUBLE_MIN_VALUE = new Double(Long.MIN_VALUE);
	public static double DOUBLE_MAX_VALUE = new Double(Long.MAX_VALUE);
	  
	public boolean filterCells[] = null;
	public String exactValCells[] = null;
	public boolean notValCells[] = null;
	public Object[] exactValCellsO = null;
	public double minValCells[] = null;
	public double maxValCells[] = null;

	public boolean inValCells[] = null;
	public String[][] inValuesA =  null;
	public Object[][] inValuesAO = null;	  
	private volatile boolean valuesParsed = false;
	String query = "";

	  /**
	   * "*|*|23|[11-12]|*|*"
	   * @param query
	   */
	public HSearchQuery(final String query) throws ParseException {
		
		if ( DEBUG_ENABLED) HSearchLog.l.debug("HSearchQuery Query :" + query);
		
		if ( null == query) throw new ParseException("Query is null.", 0);
		if ( query.length() == 0) throw new ParseException("Query is empty.", 0);

		this.query = query;
		this.load(query);
	}
	  
	  public final void parseValuesConcurrent(final String[] dataTypes) throws InvalidObjectException{
		  if ( valuesParsed ) return; 
		  synchronized (this) {
			  if ( valuesParsed ) return; 
			  parseValues(dataTypes);
			  valuesParsed = true;
		  }
	  }

	  private final void parseValues(final String[] dataTypes) throws InvalidObjectException{
	  
		  if ( null == exactValCells) return;
		  int exactValCellsT = exactValCells.length;
		  
		  if ( 0 == exactValCellsT) return;
		  if ( exactValCellsT != dataTypes.length) 
			  throw new InvalidObjectException("exactValCellsT:dataTypes.length are not matching > " + exactValCellsT + "|" + dataTypes.length );
		  
		  exactValCellsO = new Object[exactValCellsT];
		  inValuesAO = new Object[exactValCellsT][];
		  String cellData = null;
		  Object[] tempA = null;
		  int index = 0;
		  
		  try {
			  for (int i=0; i<exactValCells.length; i++) {
				  cellData = exactValCells[i];
				  if ( null == cellData && null == inValuesA[i]) continue;
				  char firstchar = dataTypes[i].charAt(0);
				  
				  if ( null != cellData ) {
					  char notChar = cellData.charAt(0);
					  if ( '!' == notChar ) {
						  notValCells[i] = true;
						  cellData = cellData.substring(1);
					  }
				  }
				  
				  index = 0;
				  switch ( firstchar) {
					  	case '*':
					  		break;
					  	case 'I':
					  		if ( null != cellData ) {
						  		exactValCellsO[i] = new Integer(cellData);
					  			
					  		} else {
					  			tempA = new Integer[inValuesA[i].length];
						  		for(String item : inValuesA[i]){
						  			tempA[index++] = new Integer(item); 
						  		}
						  		inValuesAO[i] = tempA;
					  		}
					  		break;
					  	case 'F':
					  		if( null != cellData){
						  		exactValCellsO[i] = new Float(cellData);					  			
					  		}
					  		else{
					  			tempA = new Float[inValuesA[i].length];
						  		for(String item : inValuesA[i]){
						  			tempA[index++] = new Float(item); 
						  		}
						  		inValuesAO[i] = tempA;
					  		}
					  		break;
					  	case 'D':
					  		if( null != cellData){
					  			exactValCellsO[i] = new Double(cellData);
					  		}
					  		else{
					  			tempA = new Double[inValuesA[i].length];
						  		for(String item : inValuesA[i]){
						  			tempA[index++] = new Double(item); 
						  		}
						  		inValuesAO[i] = tempA;
					  		}
					  		break;
					  	case 'L':
					  		if( null != cellData){
					  			exactValCellsO[i] = new Long(cellData);
					  		}
					  		else {
					  			tempA = new Long[inValuesA[i].length];
						  		for(String item : inValuesA[i]){
						  			tempA[index++] = new Long(item); 
						  		}
						  		inValuesAO[i] = tempA;
					  		}
					  		break;
					  	case 'S':
					  		if ( "Short".equals(dataTypes[i])) {
						  		if( null != cellData){
						  			exactValCellsO[i] = new Short(cellData);
						  		}
						  		else {
						  			tempA = new Short[inValuesA[i].length];
							  		for(String item : inValuesA[i]){
							  			tempA[index++] = new Short(item); 
							  		}
							  		inValuesAO[i] = tempA;
						  		}
					  		}
					  		else {
						  		if( null != cellData){
						  			exactValCellsO[i] = new String(cellData);
						  		}
						  		else{
						  			tempA = new String[inValuesA[i].length];
							  		for(String item : inValuesA[i]){
							  			tempA[index++] = new String(item); 
							  		}
							  		inValuesAO[i] = tempA;
						  		}
					  		}
					  		break;
					  	case 'B':
					  		if ( "Boolean".equals(dataTypes[i])){
						  		if( null != cellData){
						  			exactValCellsO[i] = new Boolean(cellData); //True or False
						  		}
						  		else{
						  			tempA = new Boolean[inValuesA[i].length];
							  		for(String item : inValuesA[i]){
							  			tempA[index++] = new Boolean(item); 
							  		}
							  		inValuesAO[i] = tempA;
						  		}
					  		}
					  		else {
						  		if( null != cellData){
						  			exactValCellsO[i] = new Byte(cellData); //Byte
						  		}
						  		else{
						  			tempA = new Byte[inValuesA[i].length];
							  		for(String item : inValuesA[i]){
							  			tempA[index++] = new Byte(item); 
							  		}
							  		inValuesAO[i] = tempA;
						  		}
					  		}
					  		break;
					  	case 'b' :
					  		if( null != cellData){
					  			exactValCellsO[i] = cellData.getBytes();
					  		}
					  		else{
						  		for(String item : inValuesA[i]){
						  			inValuesAO[i][index++] = item.getBytes();
						  		}
					  		}
					  		break;
					  }
			  	  }
			  } catch (Exception ex) {
				  String message = "HBase Filter Failure : Query = " +  this.query + "\n Cell:" + cellData + "\n" + ex.getMessage();
				  System.err.println(message);
				  ex.printStackTrace(System.err);
				  throw new InvalidObjectException(message);
			  }
	  }
	  
	  public final void load(final String text) throws ParseException  {
		  final String[] tokenizedFilters = PATTERN_PIPE_SPLITTER.split(text, -1);
		  loadAValue(tokenizedFilters);
	  }
	  
	  public final void loadAValue(final String[] filters) throws ParseException{
		  
		  int size = filters.length;
		  
		  filterCells = new boolean[size];
		  Arrays.fill(filterCells, false);
		  
		  exactValCells = new String[size];
		  Arrays.fill(exactValCells, null);
		  
		  notValCells = new boolean[size];
		  Arrays.fill(notValCells, false);
		  
		  minValCells = new double[size];
		  Arrays.fill(minValCells, DOUBLE_MIN_VALUE);

		  maxValCells = new double[size];
		  Arrays.fill(maxValCells, DOUBLE_MAX_VALUE);
		  
		  inValuesA = new String[size][];
		  Arrays.fill(inValuesA, null);
		  String inValuesStr = "";

		  inValCells = new boolean[size];
		  Arrays.fill(inValCells, false);

		  for (int seq=0; seq < size; seq++) {
			  String res = filters[seq];
			  res = PATTERN_ESCAPER.matcher(res).replaceAll("");
			  char firstChar = res.charAt(0);
			  
			  if ( firstChar == '*') {
				  filterCells[seq] = false;
				  continue;
			  }
			  filterCells[seq] = true;
			  
			  if ( firstChar == '!') {
				  notValCells[seq] = true;
				  res = res.substring(1);
			  }

			  if ( firstChar == '[') {
				  if ( res.indexOf(']') == -1) {
					  throw new ParseException("Missing Enclosure ] " + res, seq);
				  }

				  int divider = res.indexOf(RANGE_SEPARATOR);
				  if ( divider == -1) {
					  throw new ParseException("Improper range expression, Expecting " + RANGE_SEPARATOR + " as range separator.", seq);
				  }
				  

				  if ( divider > 0 ) {
					  String minS = null;
					  String maxS = null;
					  try {
						  minS = res.substring(1, divider);
						  maxS = res.substring(divider+1, res.length() - 1);
						  
						  if(!("*".equals(minS)))
							  minValCells[seq] = Double.parseDouble(minS);
						  if(!("*".equals(maxS)))
							  maxValCells[seq]  = Double.parseDouble(maxS);
						  
					  } catch ( NumberFormatException ex) {
						  throw new ParseException(res + " not parsed. Error values of max/min =  : " + minS + " with " + maxS, seq);
					  }
				  }
			  }else if ( firstChar == '{') {
				  if ( res.indexOf('}') == -1) {
					  throw new ParseException("Missing Enclosure } " + res, seq);
				  }
				  	
					inValCells[seq] = true;
					inValuesStr = res.substring(1, res.length() - 1);
					String[] values = patternCommaOutsideQuotes.split(inValuesStr);
					int valuesT = values.length;
					String value = null;
					for (int i = 0 ; i < valuesT; i++) {
						value = values[i];
						if(value.contains(QUOTES))
							values[i] = value.substring(1, value.length() - 1);
					}

				  inValuesA[seq] = values;
				  
			  }else {
				  
				  if(res.contains(QUOTES))
					  res = res.trim().substring(1, res.length() - 1);
				  exactValCells[seq] = res;
				  
			  }
		  }
	  }
}