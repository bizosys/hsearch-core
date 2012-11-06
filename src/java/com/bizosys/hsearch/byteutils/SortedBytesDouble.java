
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public final class SortedBytesDouble extends SortedBytesBase<Double> { 


	private static SortedBytesDouble singleton = new SortedBytesDouble();
	public static SortedByte<Double> getInstance() {
		return singleton;
	}
	
	private SortedBytesDouble() {
	}	

	private static final int dataSize = 8;

	@Override
	public int getSize(byte[] bytes, int offset, int length) {
		if ( null == bytes) return 0;
		return length /dataSize;
	}
	
	@Override
	public Double getValueAt(byte[] bytes, int pos) {
		return Storable.getDouble(pos*dataSize, bytes);
	}	
	
	@Override
	public Double getValueAt(byte[] bytes, int offset, int pos) {
		return Storable.getDouble(pos*dataSize + offset, bytes);
	}		
	
	@Override
	public void addAll(byte[] bytes, Collection<Double>  vals) throws IOException {
		addAll(bytes, 0, vals);
	}
	
	@Override
	public void addAll(byte[] bytes, int offset, Collection<Double>  vals) throws IOException {
		int total = (bytes.length - offset) / dataSize;
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getDouble(pos*dataSize + offset, bytes) );
		}
	}

		
	@Override
	public byte[] toBytes(Collection<Double> sortedList, boolean clearList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Double aVal : sortedList) {
			System.arraycopy(Storable.putLong(Double.doubleToLongBits(aVal)), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		return inputsB;
	}
	
	@Override
	public void getEqualToIndexes(byte[] intB, Double matchingNo, Collection<Integer> matchings) {
		getEqualToIndexes(intB, matchingNo, matchings, dataSize);
	}		
	

	@Override
	protected int compare(byte[] inputB, int offset, Double matchNo) {
		double val = Double.longBitsToDouble(Storable.getLong(offset, inputB) );
		if (val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
	}
	
	@Override
	public int getEqualToIndex(byte[] inputBytes, Double matchingNo) {
		return getEqualToIndex(inputBytes, 0, matchingNo);
	}
		
	@Override
	public int getEqualToIndex(byte[] inputBytes, int offset, Double matchNo) {
		return super.getEqualToIndex(inputBytes, offset, matchNo, dataSize);
	}
	
	@Override
	public void getGreaterThanIndexes(byte[] intB, Double matchingNo, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, false, dataSize);
	}

	@Override
	public void getGreaterThanEqualToIndexes(byte[] intB, Double matchingNo, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, true, dataSize);
	}	
	
	@Override
	public void getLessThanIndexes(byte[] intB, Double matchingNo, Collection<Integer> matchingPos ) {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, false, dataSize);
	}
	
	@Override
	public void getLessThanEqualToIndexes(byte[] intB, Double matchingNo, Collection<Integer> matchingPos) {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, true, dataSize);
	}
	
	@Override
	public void getRangeIndexes(byte[] inputData, Double matchNoStart,
			Double matchNoEnd, Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, false, dataSize);
		
	}

	@Override
	public void getRangeIndexesInclusive(byte[] inputData,
			Double matchNoStart, Double matchNoEnd, Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, true, dataSize);
	}
}
