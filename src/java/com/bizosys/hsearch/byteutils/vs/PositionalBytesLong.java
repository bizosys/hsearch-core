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

import com.bizosys.hsearch.byteutils.ByteUtil;
import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.Storable;

public final class PositionalBytesLong extends PositionalBytesBase<Long>{

	public static final ISortedByte<Long> getInstance(long defaultValue) {
		return new PositionalBytesLong(defaultValue);
	}
	
	private PositionalBytesLong(long defaultValue){
		this.dataSize = 8;
		this.defaultValue = defaultValue;
		this.defaultValueB = ByteUtil.toBytes(defaultValue);
	}
	
	@Override
	public byte[] toBytes(Collection<Long> sortedCollection)throws IOException {
		
		int maxIndex = sortedCollection.size();
		int length = maxIndex  * dataSize;
		inputBytes = new byte[length];
		int index = 0;
		for (Long input : sortedCollection) {
			if(null == input)
				System.arraycopy(defaultValueB, 0, inputBytes, index, dataSize);
			else
				System.arraycopy(ByteUtil.toBytes(input), 0, inputBytes, index, dataSize);

			index = index + dataSize;
		}
				
		return inputBytes;
	}
	

	@Override
	public Long getValueAt(int pos) throws IndexOutOfBoundsException {
		return Storable.getLong(this.offset + pos*dataSize, inputBytes);
	}

	@Override
	public int compare(byte[] inputB, int offset, Long matchNo) {
		long val = Storable.getLong(offset, inputB);
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
	}

	@Override
	public boolean isEqual(Long firstNumber, Long secondNumber) {
		return firstNumber.longValue() == secondNumber.longValue();
	}
}
