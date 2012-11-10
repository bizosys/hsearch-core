
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public class SortedBytesUnsignedShort extends SortedBytesBase<Integer>{

	private short MINIMUM_ALLOWED_LIMIT = 0;
	private int MAXIMUM_ALLOWED_LIMIT = new Integer(Short.MAX_VALUE) - (Short.MIN_VALUE - MINIMUM_ALLOWED_LIMIT);
	

	public static ISortedByte<Integer> getInstance() {
		return new SortedBytesUnsignedShort();
	}
	
	public static SortedBytesUnsignedShort getInstanceShort() {
		return new SortedBytesUnsignedShort();
	}	

	public ISortedByte<Integer> getInstanceInt() {
		return this;
	}
	
	private SortedBytesUnsignedShort() {
		this.dataSize = 2;
	}
	
	@Override
	public int getSize() {
		if ( null == this.inputBytes) return 0;
		return this.length / this.dataSize;
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
	public Integer getValueAt(int pos) {
		int val = Storable.getShort(pos*2 + offset, this.inputBytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT;
		return (val);
	}	
	
	@Override
	public void addAll(Collection<Integer>  vals) throws IOException {
		int total = getSize();
		if ( 0 == total ) return;
		
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getShort(pos*2 + offset, this.inputBytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT );
		}
	}	
	
	@Override
	public byte[] toBytes(Collection<Integer> sortedList) throws IOException {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * 2];
		
		int index = 0;
		
		for (Integer aVal : sortedList) {
			Short aValS = getShort(aVal);
			System.arraycopy(Storable.putShort(aValS), 0, inputsB, index * 2, 2);
			index++;
		}
		return inputsB;
	}
	
	@Override
	public int getEqualToIndex(Integer aVal) throws IOException {
		int matchNo = getShort(aVal);
		return super.getEqualToIndex(matchNo);
		
	}
	
	@Override
	public void getEqualToIndexes(Integer aVal, Collection<Integer> matchings) throws IOException {
		
		if ( null == this.inputBytes) return;

		short matchingNo = getShort(aVal);
		byte[] matchingNoB = Storable.putShort(matchingNo);

		int index = super.getEqualToIndex((int) matchingNo);
		if ( index == -1) return;
		
		int intBT = this.inputBytes.length / 2;
		
		//Include all matching indexes from left
		matchings.add(index);
		//System.out.println("found:" + index);
		for ( int i=index-1; i>=0; i--) {
			int pos = i * 2;
			if ( this.inputBytes[pos] !=  matchingNoB[0]) break;
			if ( this.inputBytes[pos+1] !=  matchingNoB[1]) break;
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			int pos = i * 2;
			if ( this.inputBytes[pos] !=  matchingNoB[0]) break;
			if ( this.inputBytes[pos+1] !=  matchingNoB[1]) break;
			matchings.add(i);	
			
		}
	}	
	
	
	@Override
	protected void computeRangeIndexes(Integer matchingValS, Integer matchingValE, 
			boolean isStartInclusive, boolean isEndInclusive, Collection<Integer> matchingPos) throws IOException {
		
		int compensatedMatchingValS = getShort(matchingValS);
		System.out.println( "compensatedMatchingValS : " + compensatedMatchingValS);
		super.computeRangeIndexes( (int) compensatedMatchingValS, (int) getShort(matchingValE),
				isStartInclusive, isEndInclusive, matchingPos);
	}
		
}
