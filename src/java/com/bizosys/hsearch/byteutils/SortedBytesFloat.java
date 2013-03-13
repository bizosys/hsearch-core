package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public final class SortedBytesFloat extends SortedBytesBase<Float> { 


	public static ISortedByte<Float> getInstance() {
		return new SortedBytesFloat();
	}
	
	private SortedBytesFloat() {
		this.dataSize = 4;
	}	
	
	@Override
	public final Float getValueAt(int pos) {
		return Storable.getFloat(this.offset + pos*dataSize, this.inputBytes);
	}	
	
	@Override
	public final byte[] toBytes(Collection<Float> sortedList) {

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
	protected int compare(byte[] inputB, int offset, Float matchNo) {
		float val = Float.intBitsToFloat( Storable.getInt(offset, inputB) );
		if (val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
	}
}
