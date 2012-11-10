
package com.bizosys.hsearch.byteutils;

import java.util.Collection;

public final class SortedBytesDouble extends SortedBytesBase<Double> { 


	public static ISortedByte<Double> getInstance() {
		return new SortedBytesDouble();
	}
	
	private SortedBytesDouble() {
		this.dataSize = 8;
	}	

	
	@Override
	public Double getValueAt(int pos) {
		return Storable.getDouble( this.offset + (pos * this.dataSize) , this.inputBytes);
	}	
	
		
	@Override
	public byte[] toBytes(Collection<Double> sortedList) {

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
	protected int compare(byte[] inputB, int offset, Double matchNo) {
		double val = Double.longBitsToDouble(Storable.getLong(offset, inputB) );
		if (val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
	}

}
