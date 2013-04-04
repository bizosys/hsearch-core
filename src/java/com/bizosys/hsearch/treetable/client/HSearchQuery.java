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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.bizosys.hsearch.hbase.HbaseLog;

/**
 * 	@author abinash
 */
public class HSearchQuery {

	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	
	private static final char RANGE_SEPARATOR = ':';
	private static final char FIELD_SEPARATOR = '|';
	public static double DOUBLE_MIN_VALUE = new Double(Long.MIN_VALUE);
	public static double DOUBLE_MAX_VALUE = new Double(Long.MAX_VALUE);
	  
	public boolean filterCells[] = null;
	public String exactValCells[] = null;
	public Object[] exactValCellsO = null;
	public double minValCells[] = null;
	public double maxValCells[] = null;
	  
	private volatile boolean valuesParsed = false;
	String query = "";

	  /**
	   * "*|*|23|[11-12]|*|*"
	   * @param query
	   */
	public HSearchQuery(final String query) throws ParseException {
		
		if ( DEBUG_ENABLED) HbaseLog.l.debug("HSearchQuery Query :" + query);
		
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
		  
		  String cellData = null;
		  try {
			  for (int i=0; i<exactValCells.length; i++) {
				  cellData = exactValCells[i];
				  if ( null == cellData) continue;
				  char firstchar = dataTypes[i].charAt(0);
				  switch ( firstchar) {
					  	case '*':
					  		break;
					  	case 'I':
					  		exactValCellsO[i] = new Integer(cellData);
					  		break;
					  	case 'F':
					  		exactValCellsO[i] = new Float(cellData);
					  		break;
					  	case 'D':
					  		exactValCellsO[i] = new Double(cellData);
					  		break;
					  	case 'L':
					  		exactValCellsO[i] = new Long(cellData);
					  		break;
					  	case 'S':
					  		if ( "Short".equals(dataTypes[i])) exactValCellsO[i] = new Short(cellData);
					  		else exactValCellsO[i] = new String(cellData);
					  		break;
					  	case 'B':
					  		if ( "Boolean".equals(dataTypes[i])) exactValCellsO[i] = new Boolean(cellData); //True or False
					  		else exactValCellsO[i] = new Byte(cellData.getBytes()[0]); //Byte
					  		break;
					  	case 'b' :
					  		exactValCellsO[i] = cellData.getBytes();
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
		  final List<String> tokenizedFilters = new ArrayList<String>();
		  int index1 = 0;
		  int index2 = text.indexOf(FIELD_SEPARATOR);
		  String token = null;

		  if ( index2 >= 0 ) {
			  while (index2 >= 0) {
				  token = text.substring(index1, index2);
				  tokenizedFilters.add(token);
				  index1 = index2 + 1;
				  index2 = text.indexOf(FIELD_SEPARATOR, index1);
				  if ( index2 < 0 ) index1--;
			  }
		            
			  if (index1 < text.length() - 1) {
				  tokenizedFilters.add(text.substring(index1+1));
			  }
		  } else {
			  tokenizedFilters.add(text);
		  }
		  
		  loadAValue(tokenizedFilters);
	  }
	  
	  public final void loadAValue(final List<String> filters) throws ParseException{
		  
		  int size = filters.size();
		  
		  filterCells = new boolean[size];
		  Arrays.fill(filterCells, false);
		  
		  exactValCells = new String[size];
		  Arrays.fill(exactValCells, null);
		  
		  minValCells = new double[size];
		  Arrays.fill(minValCells, DOUBLE_MIN_VALUE);

		  maxValCells = new double[size];
		  Arrays.fill(maxValCells, DOUBLE_MAX_VALUE);
		  
		  int resultT = filters.size();
		  for (int seq=0; seq < resultT; seq++) {
			  String res = filters.get(seq);
			  char firstChar = res.charAt(0);
			  
			  if ( firstChar == '*') {
				  filterCells[seq] = false;
				  continue;
			  }
			  
			  filterCells[seq] = true;
			  
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
						  minValCells[seq] = Double.parseDouble(minS);
						  maxValCells[seq] = Double.parseDouble(maxS);
					  } catch ( NumberFormatException ex) {
						  throw new ParseException(res + " not parsed. Error values of max/min =  : " + minS + " with " + maxS, seq);
					  }
				  }
			  } else {
				  exactValCells[seq] = res;
			  }
		  }
	  }
	  
}
