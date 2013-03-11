package com.bizosys.hsearch.treetable.client.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import com.bizosys.hsearch.treetable.client.HSearchQuery;


public class PartitionNumeric implements IPartition<Double> {

	public static class NumericRange {
		
		public NumericRange(double start, double end, String ext) {
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
	public void setPartitionsAndRange(String colName, String familyNames, String ranges, int partitionIndex) throws IOException {

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
	public void getMatchingFamilies(HSearchQuery query, Set<String> uniqueFamilies) throws IOException {
		
		if ( query.filterCells[this.partitionIndex]) {
			if ( null == query.exactValCellsO) {
				double min = query.minValCells[this.partitionIndex];
				double max = query.maxValCells[this.partitionIndex];
				getColumnFamilies(min, max, uniqueFamilies);
			} else {
				Object exact = query.exactValCellsO[this.partitionIndex];
				uniqueFamilies.add(getColumnFamily(new Double(exact.toString()) ));
			}
		} else {
			getColumnFamilies(HSearchQuery.DOUBLE_MIN_VALUE, HSearchQuery.DOUBLE_MAX_VALUE, uniqueFamilies);
		}
		
	}
	
	
	@Override
	public void getColumnFamilies(Double startVal, Double endVal, Set<String> families) throws IOException {
		
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
		
		if ( !isStart ) 
			throw new IOException("No matching columns found for value = " + startVal + ":" + endVal +"\n" + families.toString());

	}
	
	@Override
	public String getColumnFamily(Double exactVal) throws IOException  {

		for (NumericRange aRange : rangeL) {
			if ( aRange.start <= exactVal && aRange.end > exactVal)
					return colName + "_" + aRange.ext;
		}
		
		throw new IOException("No matching columns found for value :" + exactVal);
	}

	@Override
	public List<String> getPartitionNames() {
		return this.partitions;
	}
}
