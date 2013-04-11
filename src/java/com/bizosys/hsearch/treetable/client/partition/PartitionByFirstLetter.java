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

public final class PartitionByFirstLetter implements IPartition<String> {
	
	public static final class TextRange {
		
		public TextRange(final char start, final char end, final String ext) {
			this.ext = ext;
			this.start = start;
			this.end = end;
		}
		
		public String ext = "";
		public char start = 'a';
		public char end = 'z';
	}
	
	String colName = "";
	int partitionIndex = -1;
	List<String> partitions = new ArrayList<String>();
	List<TextRange> rangesL = new ArrayList<TextRange>();
	

	@Override
	public final void setPartitionsAndRange(final String colName, final String familyNames, 
			final String ranges, final int partitionIndex) throws IOException {

		this.colName = colName;
		StringTokenizer tokenFamily = new StringTokenizer(familyNames,",");
		while ( tokenFamily.hasMoreTokens()) {
			partitions.add(tokenFamily.nextToken()); 
		}

		StringTokenizer rangeTokens = new StringTokenizer(ranges,",");
		if ( rangeTokens.countTokens() != partitions.size() ) {
			throw new IOException("Incorrect range data in " + colName + ". " + 
				rangeTokens.countTokens() + "!=" +  partitions.size() +"\n" + 
				"rangeTokens=" + rangeTokens.toString() + "\n" + 
				"partitions=" + partitions.toString());
		}
		
		Iterator<String> familiesItr = partitions.iterator();
		while ( rangeTokens.hasMoreTokens()) {
			String aRange = rangeTokens.nextToken();
			if ( aRange.length() == 0 ) throw new IOException("Blank Range in " + colName + ". " + ranges);
			
			if ( aRange.charAt(0) != '[') {
				throw new IOException("Missing Enclosure [ " + aRange + " in " + colName + ".");
			}
			
			if ( aRange.charAt(aRange.length() - 1) != ']') {
				throw new IOException("Missing Enclosure ] " + aRange+  " in " + colName + ".");
			}
			
			int rangeI = aRange.indexOf(':');
			if ( rangeI == -1) {
				throw new IOException("Improper range expression, Expecting : as range separator." + " in " + colName + ".");
			}
			
			String rangeLStr = aRange.substring(1, rangeI);
			String rangeRStr = aRange.substring(rangeI + 1, aRange.length() - 1);
			
			char rangeL = ( "*".equals(rangeLStr) )  ? 'a' : rangeLStr.charAt(0);
			char rangeR = ( "*".equals(rangeRStr) )  ? 'z' : rangeRStr.charAt(0);
			rangesL.add(new TextRange(rangeL, rangeR, familiesItr.next()) ); 
		}
		this.partitionIndex = partitionIndex;
	}
	
	@Override
	public final void getMatchingFamilies(final HSearchQuery query, final Set<String> uniqueFamilies) throws IOException {

		if ( -1 == this.partitionIndex) throw new IOException("Partition Index is not set in the schema.");
		Object keyword = query.exactValCells[this.partitionIndex];
		if ( null == keyword) return;
	
		String keywordStr = keyword.toString();
		String familyName = getColumnFamily(keywordStr);
		uniqueFamilies.add(familyName);
	}
	
	
	@Override
	public final void getColumnFamilies(final String startVal, final String endVal, final Set<String> families) throws IOException {
		throw new IOException("No matching columns found for value = " + startVal + ":" + endVal +"\n" + families.toString());
	}
	
	@Override
	public final String getColumnFamily(String exactVal) throws IOException  {

		if ( rangesL.size() == 0 ) return colName;
		
		if ( exactVal.startsWith("-")) exactVal = exactVal.substring(1);
		
		char exactValFirst = exactVal.charAt(0);
		
		for (TextRange aRange : rangesL) {
			if ( aRange.start <= exactValFirst && aRange.end > exactValFirst)
					return colName + "_" + aRange.ext;
		}
		
		throw new IOException("No matching columns found for value :" + exactVal);
	}

	@Override
	public final List<String> getPartitionNames() {
		return this.partitions;
	}
}
