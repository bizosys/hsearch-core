package com.bizosys.hsearch.byteutils;

import java.util.Collection;

public final class SortedBytesInteger extends SortedBytesBase<Integer>{

	public static ISortedByte<Integer> getInstance() {
		return new SortedBytesInteger();
	}
	
	private SortedBytesInteger() {
		this.dataSize = 4;
	}
	
	@Override
	public Integer getValueAt(int pos) {
		return Storable.getInt(this.offset + pos*dataSize, inputBytes);
	}	
	
	@Override
	public byte[] toBytes(Collection<Integer> sortedList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Integer aVal : sortedList) {
			System.arraycopy(Storable.putInt(aVal), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		return inputsB;
	}
	
	@Override
	protected int compare(byte[] inputB, int offset, Integer matchNo) {
		int val = (inputB[offset] << 24 ) +  ( (inputB[++offset] & 0xff ) << 16 ) + 
				(  ( inputB[++offset] & 0xff ) << 8 ) + ( inputB[++offset] & 0xff );
		
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
		
	}
}
