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


	public final static ISortedByte<Float> getInstance() {
		return new SortedBytesFloat();
	}
	
	private SortedBytesFloat() {
		this.dataSize = 4;
	}	
	
	@Override
	public final Float getValueAt(final int pos) {
		int index = this.offset + pos*dataSize;
		int intVal = (inputBytes[index] << 24 ) + 
		( (inputBytes[++index] & 0xff ) << 16 ) + 
		(  ( inputBytes[++index] & 0xff ) << 8 ) + 
		( inputBytes[++index] & 0xff );
		return Float.intBitsToFloat(intVal);
	}	
	
	@Override
	public final byte[] toBytes(final Collection<Float> sortedList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Float aVal : sortedList) {
			System.arraycopy(Storable.putFloat(aVal), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		return inputsB;
	}
	
	@Override
	protected final int compare(final byte[] inputB, final int offset, final Float matchNo) {
		float val = Storable.getFloat(offset, inputB);
		if (val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
	}
}
