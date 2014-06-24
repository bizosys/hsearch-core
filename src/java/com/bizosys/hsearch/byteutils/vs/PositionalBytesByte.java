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
package com.bizosys.hsearch.byteutils.vs;

import java.io.IOException;
import java.util.Collection;

import com.bizosys.hsearch.byteutils.ISortedByte;

public final class PositionalBytesByte extends PositionalBytesBase<Byte>{

	public static final ISortedByte<Byte> getInstance(byte defaultValue) {
		return new PositionalBytesByte(defaultValue);
	}
	
	private PositionalBytesByte(byte defaultValue){
		this.dataSize = 1;
		this.defaultValue = defaultValue;
		this.defaultValueB = new byte[]{defaultValue};
	}
	
	@Override
	public byte[] toBytes(Collection<Byte> sortedCollection)throws IOException {
		
		int maxIndex = sortedCollection.size();
		int length = maxIndex  * dataSize;
		inputBytes = new byte[length];
		int index = 0;
		for (Byte input : sortedCollection) {
			if(null == input)
				inputBytes[index++] = defaultValueB[0];
			else
				inputBytes[index++] = input;
		}
				
		return inputBytes;
	}
	

	@Override
	public Byte getValueAt(int pos) throws IndexOutOfBoundsException {
		return inputBytes[this.offset + pos];
	}

	@Override
	public int compare(byte[] inputB, int offset, Byte matchNo) {
		byte val = inputB[offset];
		if ( val == matchNo.byteValue()) return 0;
		if (val > matchNo.byteValue()) return 1;
		return -1;
	}

	@Override
	public boolean isEqual(Byte firstValue, Byte secondValue) {
		return firstValue.byteValue() == secondValue.byteValue();
	}

}
