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

package com.bizosys.hsearch.treetable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesShort;
import com.bizosys.hsearch.byteutils.SortedBytesString;

public abstract class CellBase<K1>  {
	public BytesSection data;
	public ISortedByte<K1> k1Sorter = null;
	
	public abstract void parseElements() throws IOException;
	
	public final void parseElements(final byte[] data) throws IOException {
		int len = ( null == data) ? 0 : data.length;
		this.data = new BytesSection(data, 0, len);
		if ( len > 0 ) parseElements();
	}
	
	public final int indexOfKey(final K1 exactKey) throws IOException{
		if ( null == this.data) return -1;
		if ( null == exactKey ) return -1; //Nulls not allowed

		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);

		Reference keyRef = kvbytesA.getValueAtReference(0);
		if ( null == keyRef ) return -1;
		
		k1Sorter.parse(data.data, keyRef.offset, keyRef.length);
		return k1Sorter.getEqualToIndex(exactKey);
	}
	
	/**
	 * Find the index of matching range of keys
	 * @param keyMinimum
	 * @param keyMaximum
	 * @return
	 * @throws IOException
	 */
	public final Collection<Integer> indexOf(final K1 keyMinimum, final K1 keyMaximum) throws IOException{
		Collection<Integer> indexes = new ArrayList<Integer>();
		findMatchingPositions(null, keyMinimum, keyMaximum, indexes);
		return indexes;
	}
	
	protected final Reference findMatchingPositions(final K1 exactValue, 
			final K1 minimumValue, final K1 maximumValue, final Collection<Integer> foundPositions) throws IOException {
		
		if ( null == this.data) return null;
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		
		Reference keyRef = kvbytesA.getValueAtReference(0);
		if ( null == keyRef ) return null;
		
		k1Sorter.parse(data.data, keyRef.offset, keyRef.length);
		
		if ( null != exactValue || null != minimumValue || null != maximumValue ) {
				
			if ( null != exactValue ) {
				k1Sorter.getEqualToIndexes(exactValue, foundPositions);
			} else {
				if ( null != minimumValue && null != maximumValue ) {
					k1Sorter.getRangeIndexesInclusive(minimumValue, maximumValue, foundPositions);
				} else if ( null != minimumValue) {
					k1Sorter.getGreaterThanEqualToIndexes(minimumValue, foundPositions);
				} else {
					k1Sorter.getLessThanEqualToIndexes(maximumValue, foundPositions);
				}
			}
		}
		return keyRef;
	}
	
	protected final Reference findNotMatchingPositions(final K1 exactValue, final Collection<Integer> foundPositions) throws IOException {
		
		if ( null == this.data) return null;
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		
		Reference keyRef = kvbytesA.getValueAtReference(0);
		if ( null == keyRef ) return null;
		
		k1Sorter.parse(data.data, keyRef.offset, keyRef.length);
		
		if ( null != exactValue ) {
			k1Sorter.getNotEqualToIndexes(exactValue, foundPositions);
		} else {
			throw new IOException("Not is not implemented for range queries.");
		}
		return keyRef;
	}	

	protected final Reference findInMatchingPositions(final K1[] inValues, final Collection<Integer> foundPositions) throws IOException {
		
		if ( null == this.data) return null;
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		
		Reference keyRef = kvbytesA.getValueAtReference(0);
		if ( null == keyRef ) return null;
		
		k1Sorter.parse(data.data, keyRef.offset, keyRef.length);
		
		int size = inValues.length;	
		if ( 0 !=  size) {
			for(int i = 0; i < size; i++){
				k1Sorter.getEqualToIndexes(inValues[i], foundPositions);
			}
		}
		else {
			throw new IOException("Size for the in elemnts are zero.");
		}
		return keyRef;
	}	

	
	public final Collection<K1> get(final K1 exactValue) throws IOException {
		List<K1> foundKeys = new ArrayList<K1>();
		get(exactValue, null ,null, foundKeys);
		return foundKeys;
	}

	public final void get(final K1 exactValue, final Collection<K1> foundKeys ) throws IOException {
		get(exactValue, null ,null, foundKeys);
	}
	
	public final Collection<K1> get(final K1 minimumValue, final K1 maximumValue) throws IOException {
		List<K1> foundKeys = new ArrayList<K1>();
		get(null, minimumValue ,maximumValue, foundKeys);
		return foundKeys;
	}
	
	public final void get(final K1 minimumValue, final K1 maximumValue, final Collection<K1> foundKeys) throws IOException {
		get(null, minimumValue ,maximumValue, foundKeys);
	}

	private final void get(final K1 exactValue, final K1 minimumValue,
			final K1 maximumValue, final Collection<K1> foundKeys) throws IOException {

		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		
		Reference keyRef = kvbytesA.getValueAtReference(0);
		if ( null == keyRef ) return;
		
		k1Sorter.parse(data.data, keyRef.offset, keyRef.length);

		findMatchingPositions(exactValue, minimumValue, maximumValue, 
			new CellBaseFoundKeyIndex<K1>(k1Sorter, foundKeys));
	}

	public final Set<K1> keySet() throws IOException {
		Set<K1> keys = new HashSet<K1>();
		keySet(keys);
		return keys;
	}
	
	public final void keySet(final Collection<K1> keys) throws IOException {
		
		if ( null == data) {
			throw new IOException("Null Data - Use sortedList to get Keys directly");
		}

		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		
		Reference keyRef = kvbytesA.getValueAtReference(0);
		if ( null == keyRef ) return;
		
		k1Sorter.parse(data.data, keyRef.offset, keyRef.length);
		int size = k1Sorter.getSize();
		for ( int i=0; i<size; i++) {
			keys.add(k1Sorter.getValueAt(i));
		}
	}
	
	protected abstract Collection<byte[]> getEmbeddedCellBytes() throws IOException;
	protected abstract byte[] getKeyBytes() throws IOException;
	
	public final byte[] toBytes() throws IOException {

		List<byte[]> bytesElems = new ArrayList<byte[]>();

		bytesElems.add(getKeyBytes());
		byte[] valB = SortedBytesArray.getInstance().toBytes(getEmbeddedCellBytes());
		
		bytesElems.add(valB);
		byte[] cellB = SortedBytesArray.getInstance().toBytes(bytesElems);
		bytesElems.clear();
		return cellB;
	}
	
	public final byte[] remove(final K1 exactKey) throws IOException {
		return remove(exactKey, null, null);
	}
	
	public final byte[] remove(final K1 minimumValue, final K1 maximumValue) throws IOException {
		return remove(null, minimumValue, maximumValue);
	}

	private final byte[] remove(final K1 exactValue, final K1 minimumValue, final  K1 maximumValue) throws IOException {

		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		byte[] allKeysB = kvbytesA.getValueAt(0);
		
		Set<Integer> foundPositions = new HashSet<Integer>();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);
	
		k1Sorter.parse(allKeysB);
		int totalSize = k1Sorter.getSize();

		List<K1> allKeys = new ArrayList<K1>(totalSize);
		List<byte[]> allValues = new ArrayList<byte[]>(totalSize);
		ISortedByte<byte[]> sba = SortedBytesArray.getInstance();
		byte[] allValuesB = SortedBytesArray.getInstance().parse(
			data.data, data.offset, data.length).getValueAt(1);
		
		sba.parse(allValuesB);
		for (int position = 0 ; position< totalSize; position++) {
			if ( foundPositions.contains(position)) continue;
			allKeys.add(k1Sorter.getValueAt(position) );
			allValues.add(sba.getValueAt(position));
		}
		
		List<byte[]> bytesElems = new ArrayList<byte[]>();
		bytesElems.add(k1Sorter.toBytes(allKeys));
		allKeys.clear();
		bytesElems.add(sba.toBytes(allValues));
		allValues.clear();
		
		byte[] cellB = SortedBytesArray.getInstance().toBytes(bytesElems);
		return cellB;
	}
	
	public final static byte[] serializeKV(final byte[] keys, final byte[] values) throws IOException {
		List<byte[]> bytesElems = new ArrayList<byte[]>();
		bytesElems.add(keys);
		bytesElems.add(values);
		byte[] cellB = SortedBytesArray.getInstance().toBytes(bytesElems);
		bytesElems.clear();
		return cellB;		
	}
	
	@SuppressWarnings("rawtypes")
	public abstract void valuesUnchecked(K1 exactValue, K1 minimumValue, K1 maximumValue, Collection foundValues) throws IOException;

	@SuppressWarnings("rawtypes")
	public abstract void valuesUnchecked(Collection foundValues) throws IOException;
	
	
	public static final ISortedByte<?> getSorter(Class<?> type) throws IOException {
		char[] name = type.getName().toCharArray();
		char firstChar = '-';
		char secondChar = '-';
		if ( name[0] == 'j' ) {
			firstChar =  name[10];
			secondChar =  name[11];
		} else {
			firstChar =  name[0];
			secondChar =  name[1];
		}
		
		if ( (firstChar == 'D' || firstChar == 'd') && secondChar == 'o') {
			return SortedBytesDouble.getInstance();
		} else if ( (firstChar == 'L' || firstChar == 'l') && secondChar == 'o') {
			return SortedBytesLong.getInstance();
		} else if ( (firstChar == 'F' || firstChar == 'f') && secondChar == 'l') {
			return SortedBytesFloat.getInstance();
		} else if ( (firstChar == 'I' || firstChar == 'i') && secondChar == 'n') {
			return SortedBytesInteger.getInstance();
		} else if ( (firstChar == 'S' || firstChar == 's') && secondChar == 'h') {
			return SortedBytesShort.getInstance();
		} else if ( (firstChar == 'B' || firstChar == 'b') && secondChar == 'y') {
			return SortedBytesChar.getInstance();
		} else if ( (firstChar == 'B' || firstChar == 'b') && secondChar == 'o') {
			return SortedBytesBoolean.getInstance();
		} else if ( (firstChar == 'S' || firstChar == 's') && secondChar == 't') {
			return SortedBytesString.getInstance();
		} else if ( (firstChar == '[' ) && secondChar == 'B') {
			return SortedBytesArray.getInstance();
		}

		throw new IOException("Unsupported datatype - " + type.toString());
	}
}
