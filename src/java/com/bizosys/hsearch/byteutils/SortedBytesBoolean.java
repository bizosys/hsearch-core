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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import com.bizosys.hsearch.federate.BitSetWrapper;

public final class SortedBytesBoolean extends SortedBytesBase<Boolean>{

	int size = 0;
	BitSetWrapper bs = null;
	public static final ISortedByte<Boolean> getInstance() {
		return new SortedBytesBoolean();
	}
	
	public static final SortedBytesBoolean getInstanceBoolean() {
		return new SortedBytesBoolean();
	}

	private SortedBytesBoolean() {
		this.dataSize = -1;
	}	
	
	 @Override
	 public final ISortedByte<Boolean> parse(final byte[] bytes) throws IOException {
		 if ( null != bytes) {
			 this.size = Storable.getInt(0, bytes);
			 bs = BitSetWrapper.valueOf(ByteBuffer.wrap(bytes, 4, bytes.length - 4));
			 return super.parse(bytes);
		 } else {
			 this.size = 0;
			 this.bs = null;
			 return this;
		 }
	 }
	 
	 @Override
	 public final ISortedByte<Boolean> parse(final byte[] bytes, final int offset, final int length) throws IOException {
		 this.size = Storable.getInt(offset, bytes);
		 bs = BitSetWrapper.valueOf(ByteBuffer.wrap(bytes, offset + 4, length - 4));
		 return super.parse(bytes, offset, length);
	 }	

	@Override
	public final byte[] toBytes(final Collection<Boolean> sortedCollection) throws IOException {

		bs = new BitSetWrapper(sortedCollection.size());
		int index = -1;
		for (Boolean val : sortedCollection) {
			index++;
			if ( val ) bs.set(index);
		}
		
		byte[] serBits = bs.toByteArray();
		int serBitsLen = ( null == serBits) ? 0 : serBits.length;
		byte[] serComplete = new byte[4 + serBitsLen];
		
		int totalElements = sortedCollection.size();
		System.arraycopy(Storable.putInt(totalElements), 0, serComplete, 0, 4);
		System.arraycopy(serBits, 0, serComplete, 4, serBitsLen);

		return serComplete;
	}
	
	public final byte[] toBytes(final List<boolean[]> sortedList, int sortedListAT) {

		
		bs = new BitSetWrapper(sortedListAT);
		
		int index = 0;
		for (boolean[] aValA : sortedList) {
			for ( boolean aVal : aValA ) {
				bs.set(index, aVal);
				index++;
				if ( index >= sortedListAT) break;
			}
		}

		byte[] serBits = bs.toByteArray();
		int serBitsLen = ( null == serBits) ? 0 : serBits.length;
		byte[] serComplete = new byte[4 + serBitsLen];
		
		System.arraycopy(Storable.putInt(sortedListAT), 0, serComplete, 0, 4);
		System.arraycopy(serBits, 0, serComplete, 4, serBitsLen);

		return serComplete;
	}
	
	@Override
	public final int getSize() throws IOException {
		return this.size;
	}

	@Override
	public final void addAll(final Collection<Boolean> vals) throws IOException {
		
		int index = 0;

		if ( null == bs ) bs = new BitSetWrapper(vals.size());
		else index  = bs.size();
		
		for (Boolean val  : vals) {
			if ( val ) bs.set(index);
			index++;
		}
	}

	@Override
	public final Boolean getValueAt(final int pos) throws IndexOutOfBoundsException {
		return bs.get(pos);
	}

	@Override
	public final int getEqualToIndex(final Boolean matchNo) throws IOException {
		if ( matchNo ) return bs.nextSetBit(0);
		else return bs.nextClearBit(0);
	}

	@Override
	public final void getEqualToIndexes(final Boolean matchNo, final Collection<Integer> matchings) throws IOException {

		if( matchNo ){
			for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
				matchings.add(i);
			}
		} else {
			for (int i = bs.nextClearBit(0); i < this.size; i = bs.nextClearBit(i+1)) {
				matchings.add(i);
			}
		}
	}

	@Override
	public final void getNotEqualToIndexes(final Boolean matchNo, final Collection<Integer> matchings) throws IOException {

		if( matchNo ){
			for (int i = bs.nextClearBit(0); i >= 0; i = bs.nextClearBit(i+1)) {
				matchings.add(i);
			}
		} else {
			for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
				matchings.add(i);
			}
		}
	}

	@Override
	public void getGreaterThanIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getGreaterThanEqualToIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getLessThanIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getLessThanEqualToIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}
	
	@Override
	public void getRangeIndexes(Boolean matchNoStart, Boolean matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getRangeIndexesInclusive(Boolean matchNoStart, Boolean matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	protected int compare(byte[] inputB, int offset, Boolean matchNo) {
		throw new RuntimeException("Not implemented");
	}

}
