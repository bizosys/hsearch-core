
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class SortedBytesArray extends SortedBytesBase<byte[]>{

	public static ISortedByte<byte[]> getInstance() {
		return new SortedBytesArray();
	}
	
	byte[] bytes;
	int inputOffset;
	int length;
	
	private SortedBytesArray() {
	}
	
	@Override
	public ISortedByte<byte[]> parse(byte[] bytes) throws IOException {
		this.bytes = bytes;
		this.inputOffset = 0;
		this.length = ( null == bytes) ? 0 : bytes.length;
		return this;
	}

	@Override
	public ISortedByte<byte[]> parse(byte[] bytes, int offset, int length) throws IOException {
		this.bytes = bytes;
		this.inputOffset = offset;
		this.length = length;
		return this;
	}
	
	@Override
	public int getSize() {
		if ( null == bytes) return 0;
		return Storable.getInt(inputOffset, bytes);
	}
	

	@Override
	public byte[] toBytes(Collection<byte[]> sortedCollection) throws IOException {

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
	public void addAll(Collection<byte[]> vals) throws IOException {
		
		byte[] inputBytes = this.bytes;
		int offset = inputOffset;
		
		int collectionSize = Storable.getInt(offset, inputBytes);
		
		List<Integer> offsets = new ArrayList<Integer>();
		offset = offset + 4;
		
		for ( int i=0; i<collectionSize; i++) {
			int bytesLen = Storable.getInt( offset, inputBytes);
			offset = offset + 4;
			offsets.add(bytesLen);
		}
		offset = offset + 4;

		int headerOffset = offset;
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
	public byte[] getValueAt(int pos) throws IOException {
		
		byte[] inputBytes = this.bytes;
		int offset = inputOffset;
		
		int collectionSize = Storable.getInt(offset, inputBytes);
		if ( pos >= collectionSize) throw new IOException(
			"Maximum position in array is " + collectionSize + " and accessed " + pos );
		
		int elemSizeOffset = (offset + 4 + pos * 4);
		int elemStartOffset = Storable.getInt( elemSizeOffset, inputBytes);
		int elemEndOffset = Storable.getInt( elemSizeOffset + 4, inputBytes);
		//System.out.println(elemEndOffset + "-" + elemStartOffset);
		int elemLen = elemEndOffset - elemStartOffset;
		
		int headerOffset = (offset + 8 + collectionSize * 4);
		byte[] aElem = new byte[elemLen];

		System.arraycopy(inputBytes, headerOffset + elemStartOffset, aElem, 0, elemLen);
		return aElem;
	}

	@Override
	public int getEqualToIndex(byte[] matchNo) throws IOException {
		byte[] inputBytes = this.bytes;
		int offset = inputOffset;

		int collectionSize = Storable.getInt(offset, inputBytes);
		
		List<Integer> offsets = new ArrayList<Integer>();
		offset = offset + 4;
		
		for ( int i=0; i<collectionSize; i++) {
			int bytesLen = Storable.getInt( offset, inputBytes);
			offset = offset + 4;
			offsets.add(bytesLen);
		}
		
		int bodyLen = Storable.getInt(offset, inputBytes); // Find body bytes
		offsets.add(bodyLen);		
		offset = offset + 4;

		int headerOffset = offset;
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
	public void getEqualToIndexes(byte[] matchBytes, Collection<Integer> matchings) throws IOException {
		
		byte[] inputData = this.bytes;
		int offset = inputOffset;

		int collectionSize = Storable.getInt(0, inputData);
		List<Integer> offsets = new ArrayList<Integer>(collectionSize);
		offset = offset + 4;
		for ( int i=0; i<collectionSize; i++) {
			int bytesLen = Storable.getInt( offset, inputData);
			offset = offset + 4;
			offsets.add(bytesLen);
		}
		offset = offset + 4;

		int headerOffset = offset;
		offsets.add( inputData.length - headerOffset);
		
		Integer thisElemOffset = -1;
		for ( int i=0; i<collectionSize; i++) {
			thisElemOffset = offsets.get(i);
			
			boolean isSame = ByteUtil.compareBytes(inputData, headerOffset + thisElemOffset, matchBytes);
			if ( isSame ) matchings.add(i);
		}		
	}

	@Override
	public Collection<byte[]> values() throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public Collection<Integer> getEqualToIndexes(byte[] matchNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public void getGreaterThanIndexes(byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public Collection<Integer> getGreaterThanIndexes(byte[] matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public void getGreaterThanEqualToIndexes(byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public Collection<Integer> getGreaterThanEqualToIndexes(byte[] matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public void getLessThanIndexes(byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public void getLessThanEqualToIndexes(byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public Collection<Integer> getLessThanIndexes(byte[] matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public Collection<Integer> getLessThanEqualToIndexes(byte[] matchingNo) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public void getRangeIndexes(byte[] matchNoStart, byte[] matchNoEnd,
			Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public Collection<Integer> getRangeIndexes(byte[] matchNoStart,
			byte[] matchNoEnd) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public void getRangeIndexesInclusive(byte[] matchNoStart,
			byte[] matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public Collection<Integer> getRangeIndexesInclusive(byte[] matchNoStart,
			byte[] matchNoEnd) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public void getRangeIndexesInclusive(byte[] matchNoStart,
			boolean startMatch, byte[] matchNoEnd, boolean endMatch,
			Collection<Integer> matchings) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	public Collection<Integer> getRangeIndexesInclusive(byte[] matchNoStart,
			boolean startMatch, byte[] matchNoEnd, boolean endMatch) throws IOException {
		throw new IOException("Not implemented Yet");
	}

	@Override
	protected int compare(byte[] inputB, int offset, byte[] compareBytes) {

		if ( null == inputB && null == compareBytes) return 0;
		if ( null == inputB) return 1;
		if ( null == compareBytes) return -1;
		
		if ( ByteUtil.compareBytes(inputB, offset, compareBytes) ) return 0;
		else if ( inputB[0] > compareBytes[0] ) return 1;
		else return -1;
	}

}