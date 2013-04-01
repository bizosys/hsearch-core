package com.bizosys.hsearch.treetable.client.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import com.bizosys.hsearch.treetable.client.HSearchQuery;

public class PartitionByFirstLetter implements IPartition<String> {
	
	public static class TextRange {
		
		public TextRange(char start, char end, String ext) {
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
	public void setPartitionsAndRange(String colName, String familyNames, String ranges, int partitionIndex) throws IOException {

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
	public void getMatchingFamilies(HSearchQuery query, Set<String> uniqueFamilies) throws IOException {

		if ( -1 == this.partitionIndex) throw new IOException("Partition Index is not set in the schema.");
		Object keyword = query.exactValCells[this.partitionIndex];
		if ( null == keyword) return;
	
		String keywordStr = keyword.toString();
		String familyName = getColumnFamily(keywordStr);
		uniqueFamilies.add(familyName);
	}
	
	
	@Override
	public void getColumnFamilies(String startVal, String endVal, Set<String> families) throws IOException {
		throw new IOException("No matching columns found for value = " + startVal + ":" + endVal +"\n" + families.toString());
	}
	
	@Override
	public String getColumnFamily(String exactVal) throws IOException  {

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
	public List<String> getPartitionNames() {
		return this.partitions;
	}
}
