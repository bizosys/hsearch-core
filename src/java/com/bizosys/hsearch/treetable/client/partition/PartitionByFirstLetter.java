package com.bizosys.hsearch.treetable.client.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import com.bizosys.hsearch.treetable.client.HSearchQuery;

public class PartitionByFirstLetter implements IPartition<String> {
	
	public static class NumericRange {
		
		public NumericRange(char start, char end, String ext) {
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
	List<NumericRange> ranges = new ArrayList<NumericRange>();
	

	@Override
	public void setPartitionsAndRange(String colName, String familyNames, String ranges, int partitionIndex) throws IOException {

		this.colName = colName;
		StringTokenizer tokenFamily = new StringTokenizer(familyNames,",");
		while ( tokenFamily.hasMoreTokens()) {
			partitions.add(tokenFamily.nextToken()); 
		}

		List<NumericRange> rangePartitions = new ArrayList<NumericRange>();
		
		StringTokenizer rangeTokens = new StringTokenizer(ranges,",");
		if ( rangeTokens.countTokens() != partitions.size() ) {
			throw new IOException("Incorrect range data in " + colName + ". " + rangeTokens.countTokens() + "!=" +  partitions.size() );
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
			rangePartitions.add(new NumericRange(rangeL, rangeR, familiesItr.next())); 
		}
		this.partitionIndex = partitionIndex;
	}
	
	@Override
	public void getMatchingFamilies(HSearchQuery query, Set<String> uniqueFamilies) throws IOException {

		Object exact = query.exactValCellsO[this.partitionIndex];
		uniqueFamilies.add(getColumnFamily(exact.toString()));
	}
	
	
	@Override
	public void getColumnFamilies(String startVal, String endVal, Set<String> families) throws IOException {
		
		boolean isStart = false;
		
		/**
		 * suppose we have to find 30-45
		 * and ranges are 0-10 10-20  20-30  30-40  40-50  50-60 
		 */
		char startValFirst = startVal.charAt(0);
		char endValFirst = endVal.charAt(0);
		for (NumericRange aRange : ranges) {
			
			if ( isStart ) {
				if ( aRange.start >= endValFirst) break;
				families.add(colName + "_" + aRange.ext);
			} else {
				if ( aRange.start <= startValFirst && aRange.end > startValFirst) {
					isStart = true;
					families.add(colName + "_" + aRange.ext);
				}
			}
		}
		
		if ( !isStart ) 
			throw new IOException("No matching columns found for value = " + startVal + ":" + endVal +"\n" + families.toString());

	}
	
	@Override
	public String getColumnFamily(String exactVal) throws IOException  {

		char exactValFirst = exactVal.charAt(0);
		
		for (NumericRange aRange : ranges) {
			if ( aRange.start <= exactValFirst && aRange.end > exactValFirst)
					return colName + "_" + aRange.ext;
		}
		
		throw new IOException("No matching columns found for value :" + exactVal);
	}

	@Override
	public List<String> getPartitionNames() {
		return this.partitions;
	}
}
