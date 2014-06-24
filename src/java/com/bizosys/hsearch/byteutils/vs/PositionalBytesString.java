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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.bizosys.hsearch.byteutils.ByteUtil;
import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.hbase.ObjectFactory;

public final class PositionalBytesString extends PositionalBytesBase<String>{

	public static ISortedByte<String> getInstance(String defaultValue) {
		return new PositionalBytesString(defaultValue);
	}
	
	private PositionalBytesString(String defaultValue) {
		this.defaultValue = defaultValue;
		this.defaultValueB = ByteUtil.toBytes(defaultValue);
	}
	
	@Override
	public final int getSize() {
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
	public final byte[] toBytes(final Collection<String> sortedCollection) throws IOException {

		int totalEntities = sortedCollection.size();
		int seek = 0;
		byte[] headerBytes = new byte[4 + totalEntities * 4 + 4] ;
		System.arraycopy(Storable.putInt(totalEntities), 0, headerBytes, seek, 4);
		seek += 4;  //4 is added for collection size
		
		int outputBytesLen = 0;
		int index = 0;
		byte[][] bytes = new byte[totalEntities][];
		byte[] valueB = null;
		for (String value : sortedCollection) {
			
			
			System.arraycopy(Storable.putInt(outputBytesLen), 0, headerBytes, seek, 4);
			seek = seek + 4;
			if(null == value){
				
				bytes[index++] = defaultValueB;
				outputBytesLen += defaultValueB.length;
				
			} else{
				valueB = value.getBytes();
				bytes[index++] = valueB;
				outputBytesLen += valueB.length;
			}
			
		}
		System.arraycopy(Storable.putInt(outputBytesLen), 0, headerBytes, seek, 4);
		
		outputBytesLen = outputBytesLen + headerBytes.length; 
		byte[] outputBytes = new byte[outputBytesLen];
		System.arraycopy(headerBytes, 0, outputBytes, 0, headerBytes.length);
		seek = headerBytes.length;
		
		int byteSize = 0;
		for (byte[] byteArr : bytes) {
			byteSize = byteArr.length;
			System.arraycopy(byteArr, 0, outputBytes, seek, byteSize);
			seek = seek + byteSize;
		}
		return outputBytes;
	}

	@Override
	public final void addAll(final Collection<String> vals) throws IOException {
		
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
		String value = null;
		
		for ( int i=0; i<collectionSize; i++) {
			nextElemOffset = seeks.get(i+1);
			thisElemOffset = seeks.get(i);
			byte[] aElem = new byte[ nextElemOffset - thisElemOffset ];
			System.arraycopy(inputBytes, headerOffset + thisElemOffset, aElem, 0, aElem.length);
			value = new String(aElem);
			if(defaultValue.equalsIgnoreCase(value))
				continue;
			
			vals.add( value );
		}		
		ObjectFactory.getInstance().putIntegerList(seeks);
	}
	
	@Override
	public final Collection<String> values(final Collection<String> vals) throws IOException {

		if ( null == this.inputBytes ) return vals;
		int total = Storable.getInt(offset, this.inputBytes);
	  
		byte[] aElem = new byte[65536];
		Arrays.fill(aElem, (byte)0);
	  
		int elemSizeOffset = 0;
		int elemStartOffset = 0;
		int elemEndOffset = 0; 
		int elemLen = 0;	
		int headerOffset = 0;
		
		String EMPTY = "";
		String temp = null;
		
		for ( int index=0; index<total; index++) {
			elemSizeOffset = (offset + 4 + index * 4);
			elemStartOffset = Storable.getInt( elemSizeOffset, inputBytes);
			elemEndOffset = Storable.getInt( elemSizeOffset + 4, inputBytes);
			
			elemLen = elemEndOffset - elemStartOffset;
			headerOffset = (offset + 8 + total * 4);
	   
			if ( 0 == elemLen) {
				vals.add(EMPTY);
				continue;
			}
			
			if ( elemLen > 65536) {
				byte[] aElemBig = new byte[elemLen];
				System.arraycopy(inputBytes, headerOffset + elemStartOffset, aElemBig, 0, elemLen);
				temp = new String(aElemBig);
				if(this.defaultValue.equalsIgnoreCase(temp))
					continue;
				
				vals.add( temp );
				
			} else {
				System.arraycopy(inputBytes, headerOffset + elemStartOffset, aElem, 0, elemLen);
				temp = new String(aElem, 0, elemLen );
				if(this.defaultValue.equalsIgnoreCase(temp))
					continue;
				
				vals.add( temp );
			}
		}
		return vals;
	}	

	@Override
	public final String getValueAt(final int pos) throws IndexOutOfBoundsException {
		
		int collectionSize = getSize();
		if ( pos >= collectionSize) throw new IndexOutOfBoundsException(
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
	public final int getEqualToIndex(final String matchVal) throws IOException {

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

	@Override
	public final void getEqualToIndexes(final String matchVal, final Collection<Integer> matchings) throws IOException {
		getEqualOrNorEqualToIndexes(false, matchVal, matchings);
	}
	
	@Override
	public final void getNotEqualToIndexes(final String matchVal, final Collection<Integer> matchings) throws IOException {
		getEqualOrNorEqualToIndexes(true, matchVal, matchings);
	}
	
	/**
	 * Find total entieis - 4 bytes
	 * Find the end bytes position to read
	 * Iterate to find String positions
	 * Read each string
	 */
	private final void getEqualOrNorEqualToIndexes(final boolean isNot, final String matchVal, 
		final Collection<Integer> matchings) throws IOException {
		
		int collectionSize = getSize();
		if ( 0 == collectionSize) return;
		
		int headerLen = 4 + (collectionSize * 4);
		
		if (inputBytes.length < headerLen) throw new IOException(
			"Corrupted bytes : collectionSize( " + collectionSize + "), header lengh=" + headerLen + 
					" , actual length = " + inputBytes.length);
		
		List<Integer> seeks = ObjectFactory.getInstance().getIntegerList();
		
		int seek = this.offset + 4;
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
			if ( isSame ^ isNot) matchings.add(i);
		}
		
		ObjectFactory.getInstance().putIntegerList(seeks);
	}

	@Override
	public final int compare(final byte[] inputB, final int offset, final String matchNo) {
		throw new RuntimeException("Not implemened Yet");
	}

	@Override
	public boolean isEqual(String firstValue, String secondValue) {
		return firstValue.equalsIgnoreCase(secondValue);
	}
}
