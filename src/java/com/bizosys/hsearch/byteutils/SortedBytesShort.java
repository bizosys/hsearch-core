package com.bizosys.hsearch.byteutils;

import java.util.Collection;

public final class SortedBytesShort extends SortedBytesBase<Short>{

	private static SortedBytesShort singleton = new SortedBytesShort();
	public static ISortedByte<Short> getInstance() {
		return singleton;
	}
	
	private SortedBytesShort() {
		this.dataSize = 2;
	}
	
	@Override
	public Short getValueAt(int pos) {
		return Storable.getShort(this.offset + pos*dataSize, inputBytes);
	}	
	
	@Override
	public byte[] toBytes(Collection<Short> sortedList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Short aVal : sortedList) {
			System.arraycopy(Storable.putShort(aVal), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		return inputsB;
	}
	
	@Override
	protected int compare(byte[] inputB, int offset, Short matchNo) {
		int val = (  ( inputB[++offset] & 0xff ) << 8 ) + ( inputB[++offset] & 0xff );
		
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
		
	}
}
