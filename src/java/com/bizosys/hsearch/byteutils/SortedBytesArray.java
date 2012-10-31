
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class SortedBytesArray extends SortedByte<byte[]>{

	public static SortedByte<byte[]> getInstance() {
		return new SortedBytesArray();
	}
	
	private SortedBytesArray() {
	}

	@Override
	public byte[] toBytes(Collection<byte[]> sortedCollection, boolean clearList)
			throws IOException {

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
	public void addAll(byte[] inputBytes, Collection<byte[]> vals) throws IOException {
		addAll(inputBytes, 0, vals);
	}

	@Override
	public void addAll(byte[] inputBytes, int offset, Collection<byte[]> vals) throws IOException {
		
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
	public byte[] getValueAt(byte[] inputBytes, int pos) throws IOException {
		return getValueAt(inputBytes, 0, pos);
	}

	@Override
	public byte[] getValueAt(byte[] inputBytes, int offset, int pos) throws IOException {
		
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
	public int getEqualToIndex(byte[] inputData, byte[] matchNo) throws IOException {
		return getEqualToIndex(inputData, 0, matchNo);
	}

	@Override
	public int getEqualToIndex(byte[] inputBytes, int offset, byte[] matchNo) throws IOException {
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
		
		Integer thisElemOffset = -1;
		for ( int i=0; i<collectionSize; i++) {
			thisElemOffset = offsets.get(i);
			boolean isSame = ByteUtil.compareBytes(inputBytes, headerOffset + thisElemOffset, matchNo);
			if ( isSame ) return 0;
		}		
		return -1;
	}

	@Override
	public void getEqualToIndexes(byte[] inputData, byte[] matchNo,
			Collection<Integer> matchings) throws IOException {
		throw new IOException("Not available");
		
	}

	@Override
	public void getGreaterThanIndexes(byte[] inputData, byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not available");
	}

	@Override
	public void getGreaterThanEuqalToIndexes(byte[] inputData,
			byte[] matchingNo, Collection<Integer> matchingPos)
			throws IOException {
		throw new IOException("Not available");
	}

	@Override
	public void getLessThanIndexes(byte[] inputData, byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not available");
	}

	@Override
	public void getLessThanEuqalToIndexes(byte[] inputData, byte[] matchingNo,
			Collection<Integer> matchingPos) throws IOException {
		throw new IOException("Not available");
	}

	@Override
	public void getRangeIndexes(byte[] inputData, byte[] matchNoStart,
			byte[] matchNoEnd, Collection<Integer> matchings)
			throws IOException {
		throw new IOException("Not available");
		
	}

	@Override
	public void getRangeIndexesInclusive(byte[] inputData, byte[] matchNoStart,
			byte[] matchNoEnd, Collection<Integer> matchings)
			throws IOException {
		throw new IOException("Not available");
		
	}

}
