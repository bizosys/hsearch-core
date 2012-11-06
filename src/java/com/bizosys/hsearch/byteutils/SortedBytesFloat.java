
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public final class SortedBytesFloat extends SortedBytesBase<Float> { 


	private static SortedBytesFloat singleton = new SortedBytesFloat();
	public static SortedByte<Float> getInstance() {
		return singleton;
	}
	
	private SortedBytesFloat() {
	}	

	private static final int dataSize = 4;
	
	@Override
	public int getSize(byte[] bytes, int offset, int length) {
		if ( null == bytes) return 0;
		return length /dataSize;
	}
	
	
	@Override
	public Float getValueAt(byte[] bytes, int pos) {
		return Storable.getFloat(pos*dataSize, bytes);
	}	
	
	@Override
	public Float getValueAt(byte[] bytes, int offset, int pos) {
		return Storable.getFloat(pos*dataSize + offset, bytes);
	}		
	
	@Override
	public void addAll(byte[] bytes, Collection<Float>  vals) throws IOException {
		addAll(bytes, 0, vals);
	}
	
	@Override
	public void addAll(byte[] bytes, int offset, Collection<Float>  vals) throws IOException {
		int total = (bytes.length - offset) / dataSize;
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getFloat(pos*dataSize + offset, bytes) );
		}
	}

		
	@Override
	public byte[] toBytes(Collection<Float> sortedList, boolean clearList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Float aVal : sortedList) {
			System.arraycopy(Storable.putInt(Float.floatToIntBits(aVal)), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		return inputsB;
	}
	
	@Override
	public void getEqualToIndexes(byte[] intB, Float matchingNo, Collection<Integer> matchings) {
		getEqualToIndexes(intB, matchingNo, matchings, dataSize);
	}		

	@Override
	protected int compare(byte[] inputB, int offset, Float matchNo) {
		float val = Float.intBitsToFloat( Storable.getInt(offset, inputB) );
		if (val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
	}
	
	@Override
	public int getEqualToIndex(byte[] inputBytes, Float matchingNo) {
		return getEqualToIndex(inputBytes, 0, matchingNo);
	}
		
	@Override
	public int getEqualToIndex(byte[] inputBytes, int offset, Float matchNo) {
		return super.getEqualToIndex(inputBytes, offset, matchNo, dataSize);
	}
	
	@Override
	public void getGreaterThanIndexes(byte[] intB, Float matchingNo, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, false, dataSize);
	}

	@Override
	public void getGreaterThanEqualToIndexes(byte[] intB, Float matchingNo, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, true, dataSize);
	}	
	
	@Override
	public void getLessThanIndexes(byte[] intB, Float matchingNo, Collection<Integer> matchingPos ) {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, false, dataSize);
	}
	
	@Override
	public void getLessThanEqualToIndexes(byte[] intB, Float matchingNo, Collection<Integer> matchingPos) {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, true, dataSize);
	}
	
	@Override
	public void getRangeIndexes(byte[] inputData, Float matchNoStart,
			Float matchNoEnd, Collection<Integer> matchings)
			throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, false, dataSize);
		
	}

	@Override
	public void getRangeIndexesInclusive(byte[] inputData,
			Float matchNoStart, Float matchNoEnd,
			Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, true, dataSize);
		
	}
	
		
}
