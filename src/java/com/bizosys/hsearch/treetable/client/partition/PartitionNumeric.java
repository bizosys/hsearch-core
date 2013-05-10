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

package com.bizosys.hsearch.treetable.client.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import com.bizosys.hsearch.treetable.client.HSearchQuery;


public final class PartitionNumeric implements IPartition<Double> {

	public static final class NumericRange {
		
		public NumericRange(final double start, final double end, final String ext) {
			this.ext = ext;
			this.start = start;
			this.end = end;
		}
		
		public String ext = "";
		public double start = Long.MIN_VALUE;
		public double end = Long.MAX_VALUE;
	}
	
	
	String colName = "";
	int partitionIndex = -1;
	List<String> partitions = new ArrayList<String>();
	List<NumericRange> rangeL = new ArrayList<NumericRange>();
	

	@Override
	public final void setPartitionsAndRange(final String colName, final String familyNames, final String ranges, final int partitionIndex) throws IOException {

		this.colName = colName;
		StringTokenizer tokenFamily = new StringTokenizer(familyNames,",");
		while ( tokenFamily.hasMoreTokens()) {
			partitions.add(tokenFamily.nextToken()); 
		}

		StringTokenizer rangeTokens = new StringTokenizer(ranges,",");
		if ( rangeTokens.countTokens() != partitions.size() ) {
			throw new IOException("Incorrect range data. " + rangeTokens.countTokens() + "!=" +  partitions.size() );
		}
		
		Iterator<String> familiesItr = partitions.iterator();
		while ( rangeTokens.hasMoreTokens()) {
			String aRange = rangeTokens.nextToken();
			if ( aRange.length() == 0 ) throw new IOException("Blank Range " + ranges);
			
			if ( aRange.charAt(0) != '[') {
				throw new IOException("Missing Enclosure [ " + aRange);
			}
			
			if ( aRange.charAt(aRange.length() - 1) != ']') {
				throw new IOException("Missing Enclosure ] " + aRange);
			}
			
			int rangeI = aRange.indexOf(':');
			if ( rangeI == -1) {
				throw new IOException("Improper range expression, Expecting : as range separator.");
			}
			
			String rangeLStr = aRange.substring(1, rangeI);
			String rangeRStr = aRange.substring(rangeI + 1, aRange.length() - 1);
			
			double rangeLeft = ( "*".equals(rangeLStr) )  ? Long.MIN_VALUE : new Double(rangeLStr);
			double rangeRight = ( "*".equals(rangeRStr) )  ? Long.MAX_VALUE : new Double(rangeRStr);
			rangeL.add(new NumericRange(rangeLeft, rangeRight, familiesItr.next())); 
		}
		this.partitionIndex = partitionIndex;
	}
	
	@Override
	public final void getMatchingFamilies(final HSearchQuery query, final Set<String> uniqueFamilies) throws IOException {
		
		if ( query.filterCells[this.partitionIndex]) {
			
			if(query.inValCells[this.partitionIndex]){
				for (String inValue : query.inValuesA[this.partitionIndex]) {
					uniqueFamilies.add(getColumnFamily(new Double(inValue) ));					
				}
			} else {
				if(query.notValCells[this.partitionIndex]){
					getColumnFamilies(HSearchQuery.DOUBLE_MIN_VALUE, HSearchQuery.DOUBLE_MAX_VALUE, uniqueFamilies);
				} else {
					if ( null == query.exactValCells[this.partitionIndex]) {
						double min = query.minValCells[this.partitionIndex];
						double max = query.maxValCells[this.partitionIndex];
						getColumnFamilies(min, max, uniqueFamilies);
					} else {
						uniqueFamilies.add(getColumnFamily(new Double(query.exactValCells[this.partitionIndex]) ));
					}				
				}
				
			}
		} else {
			getColumnFamilies(HSearchQuery.DOUBLE_MIN_VALUE, HSearchQuery.DOUBLE_MAX_VALUE, uniqueFamilies);
		}
		
	}
	
	
	@Override
	public final void getColumnFamilies(final Double startVal, final Double endVal, final Set<String> families) throws IOException {
		
		if ( this.rangeL.size() == 0 ) {
			families.add(colName);
			return;
		}
		
		boolean isStart = false;
		
		/**
		 * suppose we have to find 30-45
		 * and ranges are 0-10 10-20  20-30  30-40  40-50  50-60 
		 */
		for (NumericRange aRange : rangeL) {
			
			if ( isStart ) {
				if ( aRange.start >= endVal) break;
				families.add(colName + "_" + aRange.ext);
			} else {
				if ( aRange.start <= startVal && aRange.end > startVal) {
					isStart = true;
					families.add(colName + "_" + aRange.ext);
				}
			}
		}
		
		if ( !isStart ) {
			System.err.println("Start is not found. Adding All : " + startVal + "\t-\t" + endVal);
			for (NumericRange aRange : rangeL) {
				families.add(colName + "_" + aRange.ext);
			}
		}
		//throw new IOException("No matching columns found for value = " + startVal + ":" + endVal +"\n" + families.toString());

	}
	
	@Override
	public final String getColumnFamily(final Double exactVal) throws IOException  {

		if ( rangeL.size() == 0 ) return colName;
		
		NumericRange lastRange = null;
		for (NumericRange aRange : rangeL) {
			if ( aRange.start <= exactVal && aRange.end > exactVal)
					return colName + "_" + aRange.ext;
			lastRange = aRange;
		}
		
		if ( lastRange.end == exactVal) return colName + "_" + lastRange.ext;
		
		throw new IOException("No matching columns found for value :" + exactVal);
	}

	@Override
	public final List<String> getPartitionNames() {
		return this.partitions;
	}
}
