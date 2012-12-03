
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.bizosys.hsearch.hbase.ObjectFactory;

public final class SortedBytesString extends SortedBytesBase<String>{

	public static ISortedByte<String> getInstance() {
		return new SortedBytesString();
	}
	
	private SortedBytesString() {
	}
	
	@Override
	public int getSize() {
		if ( null == this.inputBytes) return 0;
		return Storable.getInt(offset, this.inputBytes);
	}
	
	/**
	 * 4 bytes - total entities
	 * 4 bytes - outputBytesLen ( total * ( 4 + string length) )
	 * Each element bytes length
	 * Each element bytes
	 */
	@Override
	public byte[] toBytes(Collection<String> sortedCollection) throws IOException {

		//Total collection size, element start location, End Location
		byte[] headerBytes = new byte[4 + sortedCollection.size() * 4 + 4] ;
		System.arraycopy(Storable.putInt(sortedCollection.size()), 0, headerBytes, 0, 4);
		int seek = 4;  //4 is added for array size
		
		int outputBytesLen = 0;
		for (String bytes : sortedCollection) {

			//Populate header
			System.arraycopy(Storable.putInt(outputBytesLen), 0, headerBytes, seek, 4);
			seek = seek + 4;
			
			//Calculate Next Chunk length
			int bytesLen = ( null == bytes ) ? 0 : bytes.getBytes().length;
			outputBytesLen = outputBytesLen + bytesLen ;
			
		}
		System.arraycopy(Storable.putInt(outputBytesLen), 0, headerBytes, seek, 4);
		
		outputBytesLen = outputBytesLen + headerBytes.length; 
		byte[] outputBytes = new byte[outputBytesLen];
		System.arraycopy(headerBytes, 0, outputBytes, 0, headerBytes.length);
		seek = headerBytes.length;
		
		for (String bytes : sortedCollection) {
			int byteSize = ( null == bytes) ? 0 : bytes.getBytes().length;
			System.arraycopy(bytes.getBytes(), 0, outputBytes, seek, byteSize);
			seek = seek + byteSize;
		}
		return outputBytes;
	}

	@Override
	public void addAll(Collection<String> vals) throws IOException {
		
		int collectionSize = getSize();
		
		List<Integer> seeks = ObjectFactory.getInstance().getIntegerList();
		int seek = offset + 4;
		
		for ( int i=0; i<collectionSize; i++) {
			int bytesLen = Storable.getInt( seek, inputBytes);
			seek = seek + 4;
			seeks.add(bytesLen);
		}
		seek = seek + 4;

		int headerOffset = seek;
		seeks.add( inputBytes.length - headerOffset);
		
		Integer nextElemOffset = -1;
		Integer thisElemOffset = -1;
		for ( int i=0; i<collectionSize; i++) {
			nextElemOffset = seeks.get(i+1);
			thisElemOffset = seeks.get(i);
			byte[] aElem = new byte[ nextElemOffset - thisElemOffset ];
			System.arraycopy(inputBytes, headerOffset + thisElemOffset, aElem, 0, aElem.length);
			vals.add( new String(aElem) );
		}		
		ObjectFactory.getInstance().putIntegerList(seeks);
	}

	@Override
	public String getValueAt(int pos) throws IOException {
		
		int collectionSize = getSize();
		if ( pos >= collectionSize) throw new IOException(
			"Maximum position in array is " + collectionSize + " and accessed " + pos );
		
		int elemSizeOffset = (offset + 4 + pos * 4);
		int elemStartOffset = Storable.getInt( elemSizeOffset, inputBytes);
		int elemEndOffset = Storable.getInt( elemSizeOffset + 4, inputBytes);
		//System.out.println(elemEndOffset + "-" + elemStartOffset);
		int elemLen = elemEndOffset - elemStartOffset;
		
		int headerOffset = (offset + 8 + collectionSize * 4);
		if ( 0 == elemLen) return "";
		byte[] aElem = new byte[elemLen];

		System.arraycopy(inputBytes, headerOffset + elemStartOffset, aElem, 0, elemLen);
		return new String(aElem);
	}

	@Override
	public int getEqualToIndex(String matchVal) throws IOException {

		int collectionSize = getSize();
		if ( 0 == collectionSize) return -1;
		
		int seek = offset; 
		List<Integer> seekPositions = ObjectFactory.getInstance().getIntegerList();
		
		seek = seek + 4;
		
		for ( int i=0; i<collectionSize; i++) {
			int bytesLen = Storable.getInt(seek, inputBytes);
			seek = seek + 4;
			seekPositions.add(bytesLen);
		}
		
		int bodyLen = Storable.getInt(seek, inputBytes); // Find body bytes
		seekPositions.add(bodyLen);		
		seek = seek + 4;

		int headerOffset = seek;
		seekPositions.add( inputBytes.length - headerOffset);
		
		Integer thisElemOffset = -1;
		Integer nextElemOffset = -1;
		int elemOffset = -1;
		int elemLen = -1;
		boolean isSame = false;
		
		byte[] matchValB = matchVal.getBytes();
		for ( int i=0; i<collectionSize; i++) {
			thisElemOffset = seekPositions.get(i);
			nextElemOffset = seekPositions.get(i+1);
			elemOffset = (headerOffset + thisElemOffset);
			elemLen = nextElemOffset - thisElemOffset;
			isSame = ByteUtil.compareBytes(inputBytes, elemOffset, elemLen , matchValB);
			if ( isSame ) {
				ObjectFactory.getInstance().putIntegerList(seekPositions);
				return i;
			}
		}
		ObjectFactory.getInstance().putIntegerList(seekPositions);
		return -1;
	}

	/**
	 * Find total entieis - 4 bytes
	 * Find the end bytes position to read
	 * Iterate to find String positions
	 * Read each string
	 */
	
	@Override
	public void getEqualToIndexes(String matchVal, Collection<Integer> matchings) throws IOException {
		
		int collectionSize = getSize();
		if ( 0 == collectionSize) return;
		
		int headerLen = 4 + (collectionSize * 4);
		
		if (inputBytes.length < headerLen) throw new IOException(
			"Corrupted bytes : collectionSize( " + collectionSize + "), header lengh=" + headerLen + 
					" , actual length = " + inputBytes.length);
		
		List<Integer> seeks = ObjectFactory.getInstance().getIntegerList();
		
		int seek = 4;
		for ( int i=0; i<collectionSize; i++) {
			int bytesLen = Storable.getInt( seek, inputBytes);
			seek = seek + 4;
			seeks.add(bytesLen);
		}
		int bodyLen = Storable.getInt(seek, inputBytes); // Find body bytes
		seeks.add(bodyLen);
		
		seek = seek + 4;
		if ( (seek + bodyLen)  < headerLen) throw new IOException(
				"Corrupted bytes : collectionSize( " + collectionSize + "), body Len=" + (offset + bodyLen) + 
				" , actual length = " + inputBytes.length);
		
		int headerOffset = seek;
		Integer thisElemOffset = -1;
		Integer nextElemOffset = -1;
		byte[] matchBytes = matchVal.getBytes(); 
		
		for ( int i=0; i<collectionSize; i++) {
			thisElemOffset = seeks.get(i);
			nextElemOffset = seeks.get(i+1);
			int elemOffset = (headerOffset + thisElemOffset);
			int elemLen = nextElemOffset - thisElemOffset;
			
			boolean isSame = ByteUtil.compareBytes(inputBytes, elemOffset, elemLen,  matchBytes);
			if ( isSame ) matchings.add(i);
		}
		
		ObjectFactory.getInstance().putIntegerList(seeks);
	}

	@Override
	protected int compare(byte[] inputB, int offset, String matchNo) {
		throw new RuntimeException("Not implemened Yet");
	}
}
