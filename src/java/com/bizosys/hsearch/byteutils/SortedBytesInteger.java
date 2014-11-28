/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.byteutils;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;

public final class SortedBytesInteger extends SortedBytesBase<Integer>{

	public static final ISortedByte<Integer> getInstance() {
		return new SortedBytesInteger();
	}
	
	public static final SortedBytesInteger getInstanceInt() {
		return new SortedBytesInteger();
	}
	
	private SortedBytesInteger() {
		this.dataSize = 4;
	}
	
	@Override
	public final Integer getValueAt(final int pos) {
		return Storable.getInt(this.offset + pos*dataSize, inputBytes);
	}	
	
	@Override
	public final byte[] toBytes(final Collection<Integer> sortedList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Integer aVal : sortedList) {
			System.arraycopy(Storable.putInt(aVal), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		return inputsB;
	}
	
	public final byte[] toBytes(final List<int[]> sortedList, int sortedListAT) {

		byte[] inputsB = new byte[sortedListAT * dataSize];
		return toBytes(sortedList, sortedListAT, inputsB);
	}
	
	public final byte[] toBytes(final List<int[]> sortedList, int sortedListAT, byte[] inputsB) {

		int index = 0;
		for (int[] aValA : sortedList) {
			for ( int aVal : aValA ) {
				System.arraycopy(Storable.putInt(aVal), 0, inputsB, index * dataSize, dataSize);
				index++;
				if ( index >= sortedListAT) break;
			}
		}
		return inputsB;
	}
	public static final byte[] toBytes(final BitSet sortedList) {
		int dataSize = 4;
		int sortedListAT = sortedList.cardinality();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int count = 0;
		
		for (int i = sortedList.nextSetBit(0); i > -1; i = sortedList.nextSetBit(i+1)) {
			System.arraycopy(Storable.putInt(i), 0, inputsB, count * dataSize, dataSize);
			count++;
         }
		return inputsB;
	}

	
	@Override
	protected final int compare(final byte[] inputB, int offset, final Integer matchNo) {
		int val = (inputB[offset] << 24 ) +  ( (inputB[++offset] & 0xff ) << 16 ) + 
				(  ( inputB[++offset] & 0xff ) << 8 ) + ( inputB[++offset] & 0xff );
		
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
		
	}
}
