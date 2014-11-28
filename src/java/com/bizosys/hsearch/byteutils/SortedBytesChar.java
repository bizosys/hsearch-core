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
import java.util.List;

public final class SortedBytesChar extends SortedBytesBase<Byte>{

	public final static ISortedByte<Byte> getInstance() {
		return new SortedBytesChar();
	}

	public final static SortedBytesChar getInstanceChar() {
		return new SortedBytesChar();
	}
	
	private SortedBytesChar() {
		this.dataSize = 1;
	}
	
	@Override
	public final Byte getValueAt(int pos) {
		return inputBytes[this.offset + pos];
	}	
	
	@Override
	public final byte[] toBytes(final Collection<Byte> sortedList) {
		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT];
		
		int index = 0;
		for (byte aVal : sortedList) {
			inputsB[index] = aVal;
			index++;
		}
		return inputsB;
	}
	
	public final byte[] toBytes(final List<byte[]> sortedList, int sortedListAT) {

		byte[] inputsB = new byte[sortedListAT];
		
		int index = 0;
		for (byte[] aValA : sortedList) {
			if ( null == aValA) continue;
			int valSize = aValA.length;
			
			int nextSize = index + valSize;
			if ( nextSize < sortedListAT) {
				System.arraycopy(aValA, 0, inputsB, index, valSize);
				index = nextSize;
			} else {
				System.arraycopy(aValA, 0, inputsB, index, sortedListAT - index);
			}
		}
		return inputsB;
	}	
	
	@Override
	protected final int compare(final byte[] inputB, final int offset, final Byte matchNo) {
		byte val = inputB[offset];
		if ( val == matchNo.byteValue()) return 0;
		if (val > matchNo.byteValue()) return 1;
		return -1;
		
	}
}
