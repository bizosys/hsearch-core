
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public class SortedBytesUnsignedShort extends SortedBytesBase<Integer>{

	private short MINIMUM_ALLOWED_LIMIT = 0;
	private int MAXIMUM_ALLOWED_LIMIT = new Integer(Short.MAX_VALUE) - (Short.MIN_VALUE - MINIMUM_ALLOWED_LIMIT);
	

	private static SortedBytesUnsignedShort singleton = new SortedBytesUnsignedShort();
	public static SortedByte<Integer> getInstance() {
		return singleton;
	}
	
	public static SortedBytesUnsignedShort getInstanceShort() {
		return singleton;
	}	
	
	private static final int dataSize = 2;
	
	private SortedBytesUnsignedShort() {
	}
	
	@Override
	public int getSize(byte[] bytes, int offset, int length) {
		if ( null == bytes) return 0;
		return length /dataSize;
	}
	
	public SortedBytesUnsignedShort setMinimumValueLimit(short minVal) {
		SortedBytesUnsignedShort newProcessor = new SortedBytesUnsignedShort();
		newProcessor.MINIMUM_ALLOWED_LIMIT = minVal;
		newProcessor.MAXIMUM_ALLOWED_LIMIT = new Integer(Short.MAX_VALUE) - (Short.MIN_VALUE - newProcessor.MINIMUM_ALLOWED_LIMIT);
		return newProcessor;
	}
	
	@Override
	protected int compare(byte[] inputB, int offset, Integer matchNo) {
		
		int val = (inputB[offset] << 8) + (inputB[++offset] & 0xff);
		//System.out.println(matchNo + " supplied vs extracted " + leftNo + " @" + offset);

		if ( matchNo == val) return 0;
		if ( val > matchNo) return 1;
		return -1;
	}
	
	public short getShort(Integer aVal) throws IOException {
		if ( aVal > MAXIMUM_ALLOWED_LIMIT) throw new IOException("Suplied Value " + 
				aVal.toString() + " is greated than maximum limit : " + MAXIMUM_ALLOWED_LIMIT);

		if (aVal < MINIMUM_ALLOWED_LIMIT) throw new IOException("Suplied Value " + 
				aVal.toString() + " is less than minimum limit." + MINIMUM_ALLOWED_LIMIT);

		aVal = aVal + Short.MIN_VALUE;
		aVal = aVal - MINIMUM_ALLOWED_LIMIT;
		
		return aVal.shortValue();
	}
	
	@Override
	public Integer getValueAt(byte[] bytes, int pos) {
		int val = Storable.getShort(pos*2, bytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT;
		return (val);
	}
	
	@Override
	public Integer getValueAt(byte[] bytes, int offset, int pos) {
		int val = Storable.getShort(pos*2 + offset, bytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT;
		return (val);
	}	
	
	@Override
	public void addAll(byte[] bytes, Collection<Integer>  vals) throws IOException {
		addAll(bytes, 0, vals);
	}
	
	@Override
	public void addAll(byte[] bytes, int offset, Collection<Integer>  vals) throws IOException {
		int total = (bytes.length - offset) / 4;
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getShort(pos*2 + offset, bytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT );
		}
	}	
	
	@Override
	public byte[] toBytes(Collection<Integer> sortedList, boolean clearList) throws IOException {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * 2];
		
		int index = 0;
		
		for (Integer aVal : sortedList) {
			Short aValS = getShort(aVal);
			System.arraycopy(Storable.putShort(aValS), 0, inputsB, index * 2, 2);
			index++;
		}
		if ( clearList ) sortedList.clear();
		return inputsB;
	}
	
	@Override
	public int getEqualToIndex(byte[] intB, Integer aVal) throws IOException {
		int matchNo = getShort(aVal);
		return super.getEqualToIndex(intB, 0, matchNo, dataSize);
		
	}
	
	@Override
	public int getEqualToIndex(byte[] intB, int offset, Integer aVal) throws IOException {
		int matchNo = getShort(aVal);
		return super.getEqualToIndex(intB, offset, matchNo, dataSize);
		
	}

	@Override
	public void getEqualToIndexes(byte[] intB, Integer aVal, Collection<Integer> matchings) throws IOException {
		
		if ( null == intB) return;

		short matchingNo = getShort(aVal);
		//System.out.println("Short Value : " + matchingNo);
		byte[] matchingNoB = Storable.putShort(matchingNo);

		int index = super.getEqualToIndex(intB, 0, (int) matchingNo, dataSize);
		//System.out.println("getEqualToIndex:" + index);
		if ( index == -1) return;
		
		int intBT = intB.length / 2;
		
		//Include all matching indexes from left
		matchings.add(index);
		//System.out.println("found:" + index);
		for ( int i=index-1; i>=0; i--) {
			int pos = i * 2;
			if ( intB[pos] !=  matchingNoB[0]) break;
			if ( intB[pos+1] !=  matchingNoB[1]) break;
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			int pos = i * 2;
			if ( intB[pos] !=  matchingNoB[0]) break;
			if ( intB[pos+1] !=  matchingNoB[1]) break;
			matchings.add(i);	
			
		}
	}	
	
	@Override
	public void getLessThanEqualToIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos)  throws IOException {
		short matchingVal = getShort(matchingNo);
		computeLTLTEQIndexes(intB, (int ) matchingVal, matchingPos, true, dataSize);
	}	
	
	@Override
	public void getLessThanIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) throws IOException {
		short matchingVal = getShort(matchingNo);
		computeLTLTEQIndexes(intB, (int) matchingVal, matchingPos, false, dataSize);
	}	
	
	@Override
	public void getGreaterThanIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) throws IOException {
		short matchingVal = getShort(matchingNo);
		computeGTGTEQIndexes(intB, (int) matchingVal, matchingPos, false, dataSize);
	}	
	
	@Override
	public void getGreaterThanEqualToIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) throws IOException {
		short matchingVal = getShort(matchingNo);
		computeGTGTEQIndexes(intB, (int) matchingVal, matchingPos, true,  dataSize);
	}	
	
	@Override
	public void getRangeIndexes(byte[] inputData, Integer matchNoStart,
			Integer matchNoEnd, Collection<Integer> matchings) throws IOException {
		
		int matchingNoS = getShort(matchNoStart);
		int matchingNoE = getShort(matchNoEnd);
		computeRangeIndexes(inputData, matchingNoS, matchingNoE, matchings, false, dataSize);
	}

	@Override
	public void getRangeIndexesInclusive(byte[] inputData,
			Integer matchNoStart, Integer matchNoEnd, Collection<Integer> matchings) throws IOException {
		int matchingNoS = getShort(matchNoStart);
		int matchingNoE = getShort(matchNoEnd);
		computeRangeIndexes(inputData, matchingNoS, matchingNoE, matchings, true, dataSize);
	}	
		
}
