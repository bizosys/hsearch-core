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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class SortedBytesArray extends SortedBytesBase<byte[]>{

	public static final ISortedByte<byte[]> getInstance() {
		return new SortedBytesArray();
	}
	
	private SortedBytesArray() {
	}
	
	@Override
	public final int getSize() {
		if ( null == inputBytes) return 0;
		return Storable.getInt(offset, inputBytes);
	}
	

	@Override
	public final byte[] toBytes(final Collection<byte[]> sortedCollection) throws IOException {

		//Total collection size, element start location, End Location
		byte[] headerBytes = new byte[4 + sortedCollection.size() * 4 + 4] ;
		System.arraycopy(Storable.putInt(sortedCollection.size()), 0, headerBytes, 0, 4);
		int offset = 4;  //4 is added for array size
		
		int outputBytesLen = 0;
		for (byte[] bytes : sortedCollection) {

			//Populate header
			System.arraycopy(Storable.putInt(outputBytesLen), 0, headerBytes, offset, 4);
			offset = offset + 4;
			
			//Calculate Next Chunk length
			int bytesLen = bytes.length;
			outputBytesLen = outputBytesLen + bytesLen ;
			
		}
		System.arraycopy(Storable.putInt(outputBytesLen), 0, headerBytes, offset, 4);
		
		outputBytesLen = outputBytesLen + headerBytes.length; 
		byte[] outputBytes = new byte[outputBytesLen];
		System.arraycopy(headerBytes, 0, outputBytes, 0, headerBytes.length);
		offset = headerBytes.length;
		
		for (byte[] bytes : sortedCollection) {
			int byteSize = bytes.length;
			System.arraycopy(bytes, 0, outputBytes, offset, byteSize);
			offset = offset + byteSize;
		}
		return outputBytes;
	}

	@Override
	public final void addAll(final Collection<byte[]> vals) throws IOException {
		
		byte[] inputBytes = this.inputBytes;
		int readOffset = this.offset;
		
		int collectionSize = Storable.getInt(readOffset, inputBytes);
		
		List<Integer> offsets = new ArrayList<Integer>();
		readOffset = readOffset + 4;
		
		for ( int i=0; i<collectionSize; i++) {
			int bytesLen = Storable.getInt( readOffset, inputBytes);
			readOffset = readOffset + 4;
			offsets.add(bytesLen);
		}
		readOffset = readOffset + 4;

		int headerOffset = readOffset;
		offsets.add( inputBytes.length - headerOffset);
		
		Integer nextElemOffset = -1;
		Integer thisElemOffset = -1;
		for ( int i=0; i<collectionSize; i++) {
			nextElemOffset = offsets.get(i+1);
			thisElemOffset = offsets.get(i);
			byte[] aElem = new byte[ nextElemOffset - thisElemOffset ];
			System.arraycopy(inputBytes, headerOffset + thisElemOffset, aElem, 0, aElem.length);
			vals.add(aElem);
		}		
	}

	@Override
	public final byte[] getValueAt(final int pos) throws IndexOutOfBoundsException {
		
		byte[] inputBytes = this.inputBytes;
		int readOffset = this.offset;
		
		int collectionSize = Storable.getInt(readOffset, inputBytes);
		if ( pos >= collectionSize) throw new IndexOutOfBoundsException(
			"Maximum position in array is " + collectionSize + " and accessed " + pos );
		
		int elemSizeOffset = (readOffset + 4 + pos * 4);
		int elemStartOffset = Storable.getInt( elemSizeOffset, inputBytes);
		int elemEndOffset = Storable.getInt( elemSizeOffset + 4, inputBytes);
		//System.out.println(elemEndOffset + "-" + elemStartOffset);
		int elemLen = elemEndOffset - elemStartOffset;
		
		int headerOffset = (readOffset + 8 + collectionSize * 4);
		byte[] aElem = new byte[elemLen];

		System.arraycopy(inputBytes, headerOffset + elemStartOffset, aElem, 0, elemLen);
		return aElem;
	}

	public final Reference getValueAtReference(final int pos) {
		
		int collectionSize = Storable.getInt(this.offset, inputBytes);
		if ( pos >= collectionSize) throw new IndexOutOfBoundsException(
			"Maximum position in array is " + collectionSize + " and accessed " + pos );
		
		int elemSizeOffset = (this.offset + 4 + pos * 4);
		int elemStartOffset = Storable.getInt( elemSizeOffset, inputBytes);
		int elemEndOffset = Storable.getInt( elemSizeOffset + 4, inputBytes);
		int elemLen = elemEndOffset - elemStartOffset;
		
		int headerOffset = (this.offset + 8 + collectionSize * 4);
		return new Reference(headerOffset + elemStartOffset, elemLen);
	}	
	
	
	@Override
	public final int getEqualToIndex(byte[] matchNo) throws IOException {
		byte[] inputBytes = this.inputBytes;
		int readOffset = this.offset;

		int collectionSize = Storable.getInt(readOffset, inputBytes);
		
		List<Integer> offsets = new ArrayList<Integer>();
		readOffset = readOffset + 4;
		
		for ( int i=0; i<collectionSize; i++) {
			int bytesLen = Storable.getInt( readOffset, inputBytes);
			readOffset = readOffset + 4;
			offsets.add(bytesLen);
		}
		
		int bodyLen = Storable.getInt(readOffset, inputBytes); // Find body bytes
		offsets.add(bodyLen);		
		readOffset = readOffset + 4;

		int headerOffset = readOffset;
		offsets.add( inputBytes.length - headerOffset);
		
		Integer thisElemOffset = -1;
		Integer nextElemOffset = -1;
		int elemOffset = -1;
		int elemLen = -1;
		boolean isSame = false;
		
		for ( int i=0; i<collectionSize; i++) {
			thisElemOffset = offsets.get(i);
			nextElemOffset = offsets.get(i+1);
			elemOffset = (headerOffset + thisElemOffset);
			elemLen = nextElemOffset - thisElemOffset;
			isSame = ByteUtil.compareBytes(inputBytes, elemOffset, elemLen , matchNo);
			if ( isSame ) return i;
		}		
		return -1;
	}

	@Override
	public final void getEqualToIndexes(byte[] matchBytes, Collection<Integer> matchings) throws IOException {
		
		byte[] inputData = this.inputBytes;
		int readOffset = this.offset;

		int collectionSize = Storable.getInt(0, inputData);
		List<Integer> offsets = new ArrayList<Integer>(collectionSize);
		readOffset = readOffset + 4;
		for ( int i=0; i<collectionSize; i++) {
			int bytesLen = Storable.getInt( readOffset, inputData);
			readOffset = readOffset + 4;
			offsets.add(bytesLen);
		}
		readOffset = readOffset + 4;

		int headerOffset = readOffset;
		offsets.add( inputData.length - headerOffset);
		
		Integer thisElemOffset = -1;
		for ( int i=0; i<collectionSize; i++) {
			thisElemOffset = offsets.get(i);
			
			boolean isSame = ByteUtil.compareBytes(inputData, headerOffset + thisElemOffset, matchBytes);
			if ( isSame ) matchings.add(i);
		}		
	}

	@Override
	public final Collection<Integer> getEqualToIndexes(byte[] matchNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getGreaterThanIndexes(byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getGreaterThanIndexes(byte[] matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getGreaterThanEqualToIndexes(byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getGreaterThanEqualToIndexes(byte[] matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getLessThanIndexes(byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getLessThanEqualToIndexes(byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getLessThanIndexes(byte[] matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getLessThanEqualToIndexes(byte[] matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getRangeIndexes(byte[] matchNoStart, byte[] matchNoEnd,
			Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getRangeIndexes(byte[] matchNoStart,
			byte[] matchNoEnd) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getRangeIndexesInclusive(byte[] matchNoStart,
			byte[] matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getRangeIndexesInclusive(byte[] matchNoStart,
			byte[] matchNoEnd) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getRangeIndexesInclusive(byte[] matchNoStart,
			boolean startMatch, byte[] matchNoEnd, boolean endMatch,
			Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getRangeIndexesInclusive(byte[] matchNoStart,
			boolean startMatch, byte[] matchNoEnd, boolean endMatch) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	protected final int compare(byte[] inputB, int offset, byte[] compareBytes) {

		if ( null == inputB && null == compareBytes) return 0;
		if ( null == inputB) return 1;
		if ( null == compareBytes) return -1;
		
		if ( ByteUtil.compareBytes(inputB, offset, compareBytes) ) return 0;
		else if ( inputB[0] > compareBytes[0] ) return 1;
		else return -1;
	}

}