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

import java.io.IOException;
import java.util.Collection;

public final class SortedBytesLong extends SortedBytesBase<Long>{

	public static final ISortedByte<Long> getInstance() {
		return new SortedBytesLong();
	}
	
	private SortedBytesLong() {
		this.dataSize = 8;
	}
	
	@Override
	public final Long getValueAt(final int pos) throws IndexOutOfBoundsException {
		return Storable.getLong(this.offset + pos * dataSize, this.inputBytes);
	}

	@Override
	public final byte[] toBytes(final Collection<Long> sortedList) throws IOException {
		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Long aVal : sortedList) {
			System.arraycopy(Storable.putLong(aVal), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		
		return inputsB;
	}


	@Override
	protected final int compare(final byte[] inputB, int offset, final Long matchNo) {
		
		long val = ( ( (long) (inputB[offset]) )  << 56 )  + 
		( (inputB[++offset] & 0xffL ) << 48 ) + 
		( (inputB[++offset] & 0xffL ) << 40 ) + 
		( (inputB[++offset] & 0xffL ) << 32 ) + 
		( (inputB[++offset] & 0xffL ) << 24 ) + 
		( (inputB[++offset] & 0xff ) << 16 ) + 
		( (inputB[++offset] & 0xff ) << 8 ) + 
		( inputB[++offset] & 0xff );
		
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
		
	}		
}
