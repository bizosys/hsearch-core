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
