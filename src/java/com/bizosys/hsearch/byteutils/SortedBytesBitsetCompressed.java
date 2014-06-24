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
import java.util.ArrayList;
import java.util.Collection;

import org.iq80.snappy.Snappy;

import com.bizosys.hsearch.federate.BitSetWrapper;

public final class SortedBytesBitsetCompressed extends SortedBytesBase<BitSetWrapper>{

	SortedBytesArray sba = SortedBytesArray.getInstanceArr();
	public static final ISortedByte<BitSetWrapper> getInstance() {
		return new SortedBytesBitsetCompressed();
	}
	
	public static final SortedBytesBitsetCompressed getInstanceBitset() {
		return new SortedBytesBitsetCompressed();
	}

	private SortedBytesBitsetCompressed() {
	}
	
	@Override
	public final int getSize() {
		if ( null == inputBytes) return 0;
		return Storable.getInt(offset, inputBytes);
	}
	

	@Override
	public final byte[] toBytes(final Collection<BitSetWrapper> sortedCollection) throws IOException {

		Collection<byte[]> sortedCollectionB = new ArrayList<byte[]>(sortedCollection.size());
		for (BitSetWrapper BitSetWrapper : sortedCollection) {
			sortedCollectionB.add(BitSetWrapper.toByteArray());
		}
		return sba.toBytes(sortedCollectionB);
	}
	
	@Override
	public final void addAll(final Collection<BitSetWrapper> vals) throws IOException {
		if ( null == inputBytes) return;
		
		int collectionSize = Storable.getInt(offset, inputBytes);
		
		Reference ref = new Reference();
		for ( int i=0; i<collectionSize; i++) {
			ref = getValueAtReference(i);
			vals.add(BitSetWrapper.valueOf(ByteBuffer.wrap(inputBytes, ref.offset, ref.length)));
		}		
	}

	public final byte[] bitSetToBytes(final BitSetWrapper bits){
		 byte[] compressedOutput = Snappy.compress(bits.toByteArray());
         return compressedOutput;
	}


	public final BitSetWrapper bytesToBitSet(byte[] bytes, int offset, int length) {
		byte[] uncompressed = Snappy.uncompress(bytes, offset , length);
		return BitSetWrapper.valueOf(ByteBuffer.wrap(uncompressed, 0, uncompressed.length));
	}

	@Override
	public final BitSetWrapper getValueAt(final int pos) throws IndexOutOfBoundsException {

		int collectionSize = Storable.getInt(offset, inputBytes);
		if ( pos >= collectionSize) throw new IndexOutOfBoundsException(
			"Maximum position in array is " + collectionSize + " and accessed " + pos );
		
		Reference ref = getValueAtReference(pos);
		return BitSetWrapper.valueOf(ByteBuffer.wrap(inputBytes, ref.offset, ref.length));
	}

	public final Reference getValueAtReference(final int pos) {
		Reference ref = new Reference();
		getValueAtReference(pos, ref);
		return ref;
	}
	
	public final void getValueAtReference(final int pos, final Reference ref) {
		
		int collectionSize = Storable.getInt(this.offset, inputBytes);
		if ( pos >= collectionSize) throw new IndexOutOfBoundsException(
			"Maximum position in array is " + collectionSize + " and accessed " + pos );
		
		int elemSizeOffset = (this.offset + 4 + pos * 4);
		int elemStartOffset = Storable.getInt( elemSizeOffset, inputBytes);
		int elemEndOffset = Storable.getInt( elemSizeOffset + 4, inputBytes);
		int elemLen = elemEndOffset - elemStartOffset;
		
		int headerOffset = (this.offset + 8 + collectionSize * 4);
		ref.set(headerOffset + elemStartOffset, elemLen);
	}
	
	public static final void getKeyValueAtReference(final Reference keyRef, 
			final Reference valRef, final byte[] inputBytes , final int offset, final int length) {
		
		int collectionSize = Storable.getInt(offset, inputBytes);
		if ( collectionSize != 2 ) throw new IndexOutOfBoundsException(
			"Expected Key Value, But found " + collectionSize);
		
		int headerOffset = (offset + 16);
		int keySizeOffset = (offset + 4);
		int valSizeOffset = (offset + 8);
		
		int keyElemStartOffset = Storable.getInt( keySizeOffset, inputBytes);
		int keyElemEndOffset = Storable.getInt( keySizeOffset + 4, inputBytes);
		int elemLen = keyElemEndOffset - keyElemStartOffset;
		keyRef.set(headerOffset + keyElemStartOffset, elemLen);

		int valElemStartOffset = Storable.getInt( valSizeOffset, inputBytes);
		int valElemEndOffset = Storable.getInt( valSizeOffset + 4, inputBytes);
		elemLen = valElemEndOffset - valElemStartOffset;
		valRef.set(headerOffset + valElemStartOffset, elemLen);
	}	
	
	
	@Override
	public final int getEqualToIndex(final BitSetWrapper matchNo) throws IOException {
		if(null == this.inputBytes)
			return -1;
		
		int collectionSize = Storable.getInt(offset, inputBytes);
				
		boolean isSame = false;
		Reference ref = null;
		byte[] matchNoB = matchNo.toByteArray();
		for ( int i=0; i<collectionSize; i++) {
			ref = getValueAtReference(i);
			isSame = ByteUtil.compareBytes(inputBytes, ref.offset, ref.length, matchNoB);
			if ( isSame ) return i;
		}		
		return -1;
	}

	
	@Override
	public final void getEqualToIndexes(final BitSetWrapper matchBytes, final Collection<Integer> matchings) throws IOException {
		getEqualOrNotEqualToIndexes(false, matchBytes, matchings);
	}

	
	@Override
	public final void getNotEqualToIndexes(final BitSetWrapper matchBytes, final Collection<Integer> matchings) throws IOException {
		getEqualOrNotEqualToIndexes(true, matchBytes, matchings);
	}
	
	private final void getEqualOrNotEqualToIndexes(final boolean isNot, final BitSetWrapper matchBytes, final Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getEqualToIndexes(final BitSetWrapper matchNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getGreaterThanIndexes(final BitSetWrapper matchingNo,
			final Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getGreaterThanIndexes(BitSetWrapper matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getGreaterThanEqualToIndexes(BitSetWrapper matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getGreaterThanEqualToIndexes(BitSetWrapper matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getLessThanIndexes(BitSetWrapper matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getLessThanEqualToIndexes(BitSetWrapper matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getLessThanIndexes(BitSetWrapper matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getLessThanEqualToIndexes(BitSetWrapper matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getRangeIndexes(BitSetWrapper matchNoStart, BitSetWrapper matchNoEnd,
			Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getRangeIndexes(BitSetWrapper matchNoStart,
			BitSetWrapper matchNoEnd) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getRangeIndexesInclusive(BitSetWrapper matchNoStart,
			BitSetWrapper matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getRangeIndexesInclusive(BitSetWrapper matchNoStart,
			BitSetWrapper matchNoEnd) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getRangeIndexesInclusive(BitSetWrapper matchNoStart,
			boolean startMatch, BitSetWrapper matchNoEnd, boolean endMatch,
			Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getRangeIndexesInclusive(BitSetWrapper matchNoStart,
			boolean startMatch, BitSetWrapper matchNoEnd, boolean endMatch) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	protected int compare(byte[] inputB, int offset, BitSetWrapper matchNo) {
		return 0;
	}
}