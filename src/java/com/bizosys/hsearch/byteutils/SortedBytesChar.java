package com.bizosys.hsearch.byteutils;

import java.util.Collection;

public final class SortedBytesChar extends SortedBytesBase<Byte>{

	public static ISortedByte<Byte> getInstance() {
		return new SortedBytesChar();
	}
	
	private SortedBytesChar() {
		this.dataSize = 1;
	}
	
	@Override
	public Byte getValueAt(int pos) {
		return inputBytes[this.offset + pos];
	}	
	
	@Override
	public byte[] toBytes(Collection<Byte> sortedList) {
		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT];
		
		int index = 0;
		for (byte aVal : sortedList) {
			inputsB[index] = aVal;
			index++;
		}
		return inputsB;
	}
	
	@Override
	protected int compare(byte[] inputB, int offset, Byte matchNo) {
		byte val = inputB[offset];
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
		
	}
}
