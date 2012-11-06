
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public final class SortedBytesInteger extends SortedBytesBase<Integer>{

	private static SortedBytesInteger singleton = new SortedBytesInteger();
	public static SortedByte<Integer> getInstance() {
		return singleton;
	}
	
	private SortedBytesInteger() {
	}
	
	private static final int dataSize = 4;
	
	@Override
	public int getSize(byte[] bytes, int offset, int length) {
		if ( null == bytes) return 0;
		return length /dataSize;
	}
	
	@Override
	public Integer getValueAt(byte[] bytes, int pos) {
		return Storable.getInt(pos*dataSize, bytes);
	}	
	
	@Override
	public Integer getValueAt(byte[] bytes, int offset, int pos) {
		return Storable.getInt(pos*dataSize + offset, bytes);
	}	
	
	@Override
	public void addAll(byte[] bytes, Collection<Integer>  vals) throws IOException {
		addAll(bytes, 0, vals);
	}
	
	@Override
	public void addAll(byte[] bytes, int offset, Collection<Integer>  vals) throws IOException {
		int total = (bytes.length - offset) / dataSize;
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getInt(pos*dataSize + offset, bytes) );
		}
	}	

	@Override
	public byte[] toBytes(Collection<Integer> sortedList, boolean clearList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Integer aVal : sortedList) {
			System.arraycopy(Storable.putInt(aVal), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		
		if  ( clearList ) sortedList.clear();
		return inputsB;
	}
	
	@Override
	public void getEqualToIndexes(byte[] intB, Integer matchNo, Collection<Integer> matchings) {

		if ( null == intB) return;
		int intBT = intB.length / dataSize;
		if ( 0 == intBT) return;
		
		byte[] matchingNoB = Storable.putInt(matchNo);
		
		int index = getEqualToIndex(intB, 0, matchNo);
		//System.out.println("index:" + index);
		if ( index == -1) return ;

		//Include all matching indexes from left
		matchings.add(index);
		//System.out.println("First Index:" + index);
		for ( int i=index-1; i>=0; i--) {
			int pos = i * dataSize;
			if ( intB[pos] !=  matchingNoB[0]) break;
			if ( intB[pos+1] !=  matchingNoB[1]) break;
			if ( intB[pos+2] !=  matchingNoB[2]) break;
			if ( intB[pos+3] !=  matchingNoB[3]) break;
			//System.out.println("left:" + i);
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			int pos = i * dataSize;
			if ( intB[pos] !=  matchingNoB[0]) break;
			if ( intB[pos+1] !=  matchingNoB[1]) break;
			if ( intB[pos+2] !=  matchingNoB[2]) break;
			if ( intB[pos+3] !=  matchingNoB[3]) break;
			//System.out.println("right:" + i);
			matchings.add(i);	
			
		}
	}	

	@Override
	protected int compare(byte[] inputB, int offset, Integer matchNo) {
		
		//System.out.println(Storable.getInt(offset, inputB) + " supplied vs extracted " + matchNo + " @" + offset);
		
		int val = (inputB[offset] << 24 ) +  ( (inputB[++offset] & 0xff ) << 16 ) + 
				(  ( inputB[++offset] & 0xff ) << 8 ) + ( inputB[++offset] & 0xff );
		
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
		
	}
	
	@Override
	public int getEqualToIndex(byte[] inputBytes, Integer matchingNo) {
		return getEqualToIndex(inputBytes, 0, matchingNo);
	}
		
	@Override
	public int getEqualToIndex(byte[] inputBytes, int offset, Integer matchNo) {
		return super.getEqualToIndex(inputBytes, offset, matchNo, dataSize);
	}
	
	@Override
	public void getGreaterThanIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, false, dataSize);
	}

	@Override
	public void getGreaterThanEqualToIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, true, dataSize);
	}	
	
	@Override
	public void getLessThanIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos ) {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, false, dataSize);
	}
	
	@Override
	public void getLessThanEqualToIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, true, dataSize);
	}
	
	@Override
	public void getRangeIndexes(byte[] inputData, Integer matchNoStart,
			Integer matchNoEnd, Collection<Integer> matchings)
			throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, false, dataSize);
		
	}

	@Override
	public void getRangeIndexesInclusive(byte[] inputData,
			Integer matchNoStart, Integer matchNoEnd,
			Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, true, dataSize);
		
	}
}
