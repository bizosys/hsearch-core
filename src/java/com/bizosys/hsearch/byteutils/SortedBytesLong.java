package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public final class SortedBytesLong extends SortedBytesBase<Long>{

	private static SortedBytesLong singleton = new SortedBytesLong();
	public static SortedByte<Long> getInstance() {
		return singleton;
	}
	
	private SortedBytesLong() {
	}
	
	private static final int dataSize = 8;
	
	@Override
	public int getSize(byte[] bytes, int offset, int length) {
		if ( null == bytes) return 0;
		return length /dataSize;
	}
	
	@Override
	public Long getValueAt(byte[] bytes, int pos) throws IOException {
		return Storable.getLong(pos*dataSize, bytes);
	}

	@Override
	public Long getValueAt(byte[] bytes, int offset, int pos)
			throws IOException {
		return Storable.getLong(pos*dataSize + offset, bytes);
	}
	
	@Override
	public void addAll(byte[] bytes, Collection<Long> vals) throws IOException {
		addAll(bytes, 0, vals);
	}

	@Override
	public void addAll(byte[] bytes, int offset, Collection<Long> vals)
			throws IOException {
		int total = (bytes.length - offset) / dataSize;
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getLong(pos*dataSize + offset, bytes) );
		}		
	}

	@Override
	public byte[] toBytes(Collection<Long> sortedList, boolean clearList)
			throws IOException {
		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Long aVal : sortedList) {
			System.arraycopy(Storable.putLong(aVal), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		
		if  ( clearList ) sortedList.clear();
		return inputsB;
	}


	@Override
	public void getEqualToIndexes(byte[] intB, Long matchNo, Collection<Integer> matchings) throws IOException {
		if ( null == intB) return;
		int intBT = intB.length / dataSize;
		if ( 0 == intBT) return;
		
		byte[] matchingNoB = Storable.putLong(matchNo);
		
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
			if ( intB[pos+4] !=  matchingNoB[4]) break;
			if ( intB[pos+5] !=  matchingNoB[5]) break;
			if ( intB[pos+6] !=  matchingNoB[6]) break;
			if ( intB[pos+7] !=  matchingNoB[7]) break;
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			int pos = i * dataSize;
			if ( intB[pos] !=  matchingNoB[0]) break;
			if ( intB[pos+1] !=  matchingNoB[1]) break;
			if ( intB[pos+2] !=  matchingNoB[2]) break;
			if ( intB[pos+3] !=  matchingNoB[3]) break;
			if ( intB[pos+4] !=  matchingNoB[4]) break;
			if ( intB[pos+5] !=  matchingNoB[5]) break;
			if ( intB[pos+6] !=  matchingNoB[6]) break;
			if ( intB[pos+7] !=  matchingNoB[7]) break;
			matchings.add(i);	
			
		}		
	}
	
	@Override
	protected int compare(byte[] inputB, int offset, Long matchNo) {
		
		long val = ( ( (long) (inputB[offset]) )  << 56 )  + 
		( (inputB[++offset] & 0xffL ) << 48 ) + 
		( (inputB[++offset] & 0xffL ) << 40 ) + 
		( (inputB[++offset] & 0xffL ) << 32 ) + 
		( (inputB[++offset] & 0xffL ) << 24 ) + 
		( (inputB[++offset] & 0xff ) << 16 ) + 
		( (inputB[++offset] & 0xff ) << 8 ) + 
		( inputB[++offset] & 0xff );
		
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
		
	}		
	
	@Override
	public int getEqualToIndex(byte[] inputBytes, Long matchingNo) throws IOException {
		return getEqualToIndex(inputBytes, 0, matchingNo);
	}

	@Override
	public int getEqualToIndex(byte[] inputBytes, int offset, Long matchNo) throws IOException {
		return getEqualToIndex(inputBytes, offset, matchNo, dataSize);
	}
	

	@Override
	public void getGreaterThanIndexes(byte[] inputData, Long matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		computeGTGTEQIndexes(inputData, matchingNo, matchingPos, false, dataSize);
		
	}

	@Override
	public void getGreaterThanEqualToIndexes(byte[] inputData, Long matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		computeGTGTEQIndexes(inputData, matchingNo, matchingPos, true, dataSize);
		
	}
	
	@Override
	public void getLessThanIndexes(byte[] intB, Long matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, false, dataSize);
		
	}

	@Override
	public void getLessThanEqualToIndexes(byte[] intB, Long matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, true, dataSize);
	}
	
	@Override
	public void getRangeIndexes(byte[] inputData, Long matchNoStart,
			Long matchNoEnd, Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, false, dataSize);
		
	}

	@Override
	public void getRangeIndexesInclusive(byte[] inputData, Long matchNoStart,
			Long matchNoEnd, Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, true, dataSize);
		
	}
}
