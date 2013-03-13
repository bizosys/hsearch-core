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

public final class SortedBytesShort extends SortedBytesBase<Short>{

	public static ISortedByte<Short> getInstance() {
		return new SortedBytesShort();
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
		int val = (  ( inputB[offset] & 0xff ) << 8 ) + ( inputB[++offset] & 0xff );
		
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
		
	}
}
