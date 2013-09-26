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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

public final class SortedBytesBitset extends SortedBytesBase<BitSet>{

	public static final ISortedByte<BitSet> getInstance() {
		return new SortedBytesBitset();
	}
	
	public static final SortedBytesBitset getInstanceBitset() {
		return new SortedBytesBitset();
	}

	private SortedBytesBitset() {
	}
	
	@Override
	public final int getSize() {
		if ( null == inputBytes) return 0;
		return Storable.getInt(offset, inputBytes);
	}
	

	@Override
	public final byte[] toBytes(final Collection<BitSet> sortedCollection) throws IOException {

		//Total collection size, element start location, End Location
		byte[] headerBytes = new byte[4 + sortedCollection.size() * 4 + 4] ;
		System.arraycopy(Storable.putInt(sortedCollection.size()), 0, headerBytes, 0, 4);
		int offset = 4;  //4 is added for array size
		
		int outputBytesLen = 0;
		for (BitSet bytes : sortedCollection) {

			//Populate header
			System.arraycopy(Storable.putInt(outputBytesLen), 0, headerBytes, offset, 4);
			offset = offset + 4;
			
			//Calculate Next Chunk length
			int bytesLen = bitSetToBytesLen(bytes);
			outputBytesLen = outputBytesLen + bytesLen ;
			
		}
		System.arraycopy(Storable.putInt(outputBytesLen), 0, headerBytes, offset, 4);
		
		outputBytesLen = outputBytesLen + headerBytes.length; 
		byte[] outputBytes = new byte[outputBytesLen];
		System.arraycopy(headerBytes, 0, outputBytes, 0, headerBytes.length);
		offset = headerBytes.length;
		
		for (BitSet bytes : sortedCollection) {
			int byteSize = bitSetToBytesLen(bytes);
			System.arraycopy(bitSetToBytes(bytes), 0, outputBytes, offset, byteSize);
			offset = offset + byteSize;
		}
		return outputBytes;
	}
	

	@Override
	public final void addAll(final Collection<BitSet> vals) throws IOException {
		
		if ( null == inputBytes) return;
		
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
		
		int thisElemOffset = -1;
		for ( int i=0; i<collectionSize; i++) {
			thisElemOffset = offsets.get(i);
			vals.add(bytesToBitSet(inputBytes, headerOffset + thisElemOffset));
		}		
	}

	@Override
	public final BitSet getValueAt(final int pos) throws IndexOutOfBoundsException {
		
		byte[] inputBytes = this.inputBytes;
		int readOffset = this.offset;
		
		int collectionSize = Storable.getInt(readOffset, inputBytes);
		if ( pos >= collectionSize) throw new IndexOutOfBoundsException(
			"Maximum position in array is " + collectionSize + " and accessed " + pos );
		
		int elemSizeOffset = (readOffset + 4 + pos * 4);
		int elemStartOffset = Storable.getInt( elemSizeOffset, inputBytes);
		int elemEndOffset = Storable.getInt( elemSizeOffset + 4, inputBytes);
		int elemLen = elemEndOffset - elemStartOffset;
		
		int headerOffset = (readOffset + 8 + collectionSize * 4);
		return bytesToBitSet(inputBytes, headerOffset + elemStartOffset);
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
	public final int getEqualToIndex(final BitSet matchNo) throws IOException {
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
			isSame = ByteUtil.compareBytes(inputBytes, elemOffset, elemLen, bitSetToBytes(matchNo));
			if ( isSame ) return i;
		}		
		return -1;
	}

	
	@Override
	public final void getEqualToIndexes(final BitSet matchBytes, final Collection<Integer> matchings) throws IOException {
		getEqualOrNotEqualToIndexes(false, matchBytes, matchings);
	}

	
	@Override
	public final void getNotEqualToIndexes(final BitSet matchBytes, final Collection<Integer> matchings) throws IOException {
		getEqualOrNotEqualToIndexes(true, matchBytes, matchings);
	}
	
	private final void getEqualOrNotEqualToIndexes(final boolean isNot, final BitSet matchBytes, final Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getEqualToIndexes(final BitSet matchNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getGreaterThanIndexes(final BitSet matchingNo,
			final Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getGreaterThanIndexes(BitSet matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getGreaterThanEqualToIndexes(BitSet matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getGreaterThanEqualToIndexes(BitSet matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getLessThanIndexes(BitSet matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getLessThanEqualToIndexes(BitSet matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getLessThanIndexes(BitSet matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getLessThanEqualToIndexes(BitSet matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getRangeIndexes(BitSet matchNoStart, BitSet matchNoEnd,
			Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getRangeIndexes(BitSet matchNoStart,
			BitSet matchNoEnd) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getRangeIndexesInclusive(BitSet matchNoStart,
			BitSet matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getRangeIndexesInclusive(BitSet matchNoStart,
			BitSet matchNoEnd) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final void getRangeIndexesInclusive(BitSet matchNoStart,
			boolean startMatch, BitSet matchNoEnd, boolean endMatch,
			Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public final Collection<Integer> getRangeIndexesInclusive(BitSet matchNoStart,
			boolean startMatch, BitSet matchNoEnd, boolean endMatch) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	private final int bitSetToBytesLen(final BitSet bits) throws IOException {
		int available = bits.size();
		int packed = available/8;
		int remaining = available - packed * 8;
		
		int neededBytes = packed;
		if ( remaining > 0 ) neededBytes++;
		
		neededBytes = neededBytes + 4; // How many
		return neededBytes;
	}
	
	private final byte[] bitSetToBytes(final BitSet bits) throws IOException {
		int available = bits.size();
		int packed = available/8;
		int remaining = available - packed * 8;
		
		int neededBytes = packed;
		if ( remaining > 0 ) neededBytes++;
		
		neededBytes = neededBytes + 4; // How many
		
		byte[] out = new byte[neededBytes];
		
		
		System.arraycopy(Storable.putInt(available), 0, out, 0, 4);
		
		int bitIndex = 0;
		for ( int i=0; i<packed; i++) {
			out[4+i]  = ByteUtil.fromBits(new boolean[] {
				bits.get(bitIndex++), bits.get(bitIndex++), bits.get(bitIndex++), bits.get(bitIndex++),
				bits.get(bitIndex++), bits.get(bitIndex++), bits.get(bitIndex++), bits.get(bitIndex++)});
		}
		
		if ( remaining > 0 ) {
			boolean[] remainingBits = new boolean[8];
			Arrays.fill(remainingBits, true);
			for ( int j=0; j<remaining; j++) {
				remainingBits[j] = bits.get(bitIndex++);
			}
				
			out[4+packed] = ByteUtil.fromBits(remainingBits);
		}
		return out;
	}
	

	public final BitSet bytesToBitSet(byte[] bits, int bitsOffset) throws IndexOutOfBoundsException {
		if ( null == bits) return null;
		
		int available = Storable.getInt(bitsOffset, bits);
		int packed = available/8;
		int remaining = available - packed * 8;
		
		BitSet output = new BitSet(available);
		int seq = 0;
		for (int i=0; i<packed; i++) {
			for (boolean val : Storable.byteToBits(bits[bitsOffset + 4 + i])) {
				if ( val ) output.set(seq);
				seq++;
				if ( available < seq) break;
			}
		}
		
		if ( remaining > 0 ) {
			boolean[] x = Storable.byteToBits(this.inputBytes[offset + 4 + packed]);
			for ( int i=0; i<remaining; i++) {
				if ( x[i] ) output.set(seq);
				seq++;
				if ( available < seq) break;
			}
		}
		
		return output;
	}

	@Override
	protected int compare(byte[] inputB, int offset, BitSet matchNo) {
		return 0;
	}
}