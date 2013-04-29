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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.hbase.ObjectFactory;

public class Cell2<K1, V> {
	
	public ISortedByte<K1> k1Sorter = null;
	public ISortedByte<V> vSorter = null;
	public List<CellKeyValue<K1, V>> sortedList = null;
	public BytesSection data = null;
	
	public Cell2(final ISortedByte<K1> k1Sorter, final ISortedByte<V> vSorter) {
		this.k1Sorter = k1Sorter;
		this.vSorter = vSorter;
	}

	public Cell2(final ISortedByte<K1> k1Sorter, final ISortedByte<V> vSorter, final List<CellKeyValue<K1, V>> sortedList) {
		this(k1Sorter, vSorter);
		this.sortedList = sortedList;
	}

	
	public Cell2 (final ISortedByte<K1> k1Sorter, final ISortedByte<V> vSorter, final byte[] data) {
		this(k1Sorter, vSorter);
		int dataLen = ( null == data) ? 0 : data.length;
		this.data = new BytesSection(data, 0, dataLen);
	}
	
	public Cell2 (final ISortedByte<K1> k1Sorter, final ISortedByte<V> vSorter, final BytesSection sectionData) {
		this(k1Sorter, vSorter);
		this.data = sectionData;
	}

	public final void add(final K1 k, final V v) {
		if ( null == sortedList) sortedList = new ArrayList<CellKeyValue<K1,V>>();
		sortedList.add(new CellKeyValue<K1, V>(k, v) );
	}
	
	public final void sort(final Comparator<CellKeyValue<K1, V>> comp) {
		if ( null == sortedList) return;
		Collections.sort(sortedList, comp);
	}
	
	/**
	 * Visits all data
	 * @param visitor
	 * @throws IOException
	 */
	public final void process(final Cell2Visitor<K1,V> visitor) throws IOException{
		Reference keyRef = new Reference();
		Reference valRef = new Reference();
		SortedBytesArray.getKeyValueAtReference(keyRef, valRef, data.data, data.offset, data.length);
		
		int sizeK = k1Sorter.parse(data.data, keyRef.offset, keyRef.length).getSize();
		int sizeV = vSorter.parse(data.data, valRef.offset, valRef.length).getSize();
		if ( sizeK != sizeV ) throw new IOException("Not a unique Key " + sizeK + " != " + sizeV);
		
		for ( int i=0; i<sizeK; i++) {
			visitor.visit(k1Sorter.getValueAt(i), vSorter.getValueAt(i));
		}		
	}	
		
	/**
	 * Visits only matching rows
	 * @param exactValue
	 * @param minimumValue
	 * @param maximumValue
	 * @param reusableFoundPosArray
	 * @param visitor
	 * @throws IOException
	 */
	public final void process(final V exactValue, final V minimumValue, final V maximumValue, final Cell2Visitor<K1,V> visitor) throws IOException {
		
		Reference keyRef = new Reference();
		Reference valRef = new Reference();

		SortedBytesArray.getKeyValueAtReference(keyRef, valRef, data.data, data.offset, data.length);
		
		int sizeK = k1Sorter.parse(data.data, keyRef.offset, keyRef.length).getSize();
		int sizeV = vSorter.parse(data.data, valRef.offset, valRef.length).getSize();
		if ( sizeK != sizeV ) throw new IOException("Not a unique Key " + sizeK + " != " + sizeV);
		if ( null != exactValue || null != minimumValue || null != maximumValue) {
			findMatchingPositionsVsorterInitialized(
					exactValue, minimumValue, maximumValue, 
					new Cell2FoundIndex<K1, V>(k1Sorter, vSorter, visitor) );
		} else {
			for ( int i=0; i<sizeK; i++) {
				visitor.visit(k1Sorter.getValueAt(i), vSorter.getValueAt(i));
			}
		}
	}	
	
	public final void processNot(final V exactValue, final Cell2Visitor<K1,V> visitor) throws IOException {
		Reference keyRef = new Reference();
		Reference valRef = new Reference();
		SortedBytesArray.getKeyValueAtReference(keyRef, valRef, data.data, data.offset, data.length);

		int sizeK = k1Sorter.parse(data.data, keyRef.offset, keyRef.length).getSize();
		int sizeV = vSorter.parse(data.data, valRef.offset, valRef.length).getSize();
		if ( sizeK != sizeV ) throw new IOException("Not a unique Key");
		if ( null != exactValue ) {
			findNonMatchingPositionsVsorterInitialized( exactValue, 
				new Cell2FoundIndex<K1, V>(k1Sorter, vSorter, visitor) );
		} else {
			throw new IOException("Not queries are not yet supported for ranges.");
		}		
	}
	
	
	public final List<CellKeyValue<K1, V>> getMap(final byte[] data) throws IOException {
		int dataLen = ( null == data) ? 0 : data.length;
		this.data = new BytesSection(data, 0, dataLen) ;
		parseElements();
		return sortedList;
	}
	
	public final List<CellKeyValue<K1, V>> getMap() throws IOException {
		if ( null != sortedList) return sortedList;
		if ( null != this.data) {
			parseElements();
			return sortedList;
		}
		throw new IOException("Cell is not initialized");
	}
	
	public final void getMap(final List<CellKeyValue<K1, V>> valueContainer) throws IOException {
		if ( null != sortedList) {
			valueContainer.addAll(sortedList);
			return;
		}
		
		this.sortedList = valueContainer;
		if ( null != this.data) {
			parseElements();
			return;
		}
		throw new IOException("Cell is not initialized");
	}	
	
	public final void getMap(final List<K1> kContainer, final List<V> vContainer) throws IOException{
		keySet(kContainer);
		values(vContainer);
	}
	
	public final void getMap(final V exactValue, final V minimumValue, final V maximumValue, 
			final List<Integer> reusableFoundPosArray, final List<K1> kContainer, final List<V> vContainer) throws IOException {
		
		List<Integer> foundPositions = reusableFoundPosArray;
		byte[] allValsB = findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);
		byte[] allKeysB = SortedBytesArray.getInstance().parse(
			data.data, data.offset, data.length).getValueAt(0);
		
		ISortedByte<V> valSorted =  vSorter.parse(allValsB);
		ISortedByte<K1> keySorted =  k1Sorter.parse(allKeysB);
		
		for (int position : foundPositions) {
			kContainer.add(keySorted.getValueAt(position));
			vContainer.add(valSorted.getValueAt(position));
		}
	}		
	
	public final void populate(final Map<K1,V> map) throws IOException {
		Reference keyRef = new Reference();
		Reference valRef = new Reference();
		SortedBytesArray.getKeyValueAtReference(keyRef, valRef, data.data, data.offset, data.length);

		k1Sorter.parse(data.data, keyRef.offset, keyRef.length);
		vSorter.parse(data.data, valRef.offset, valRef.length);
		
		int sizeK = k1Sorter.getSize();
		int sizeV = vSorter.getSize();
		
		if ( sizeK != sizeV) {
			
			StringBuilder errState = new StringBuilder();
			errState.append("Mismatch keys : " + sizeK + " , and values = " + sizeV + "\n" );
			for ( int i=0; i<sizeK; i++) {
				//errState.append(k1Sorter.getValueAt(i));
			}			
			
			for ( int i=0; i<sizeV; i++) {
				errState.append(vSorter.getValueAt(i));
			}			
			System.err.println(errState.toString());
			
			throw new IOException(errState.toString());
			
		}
		
		for ( int i=0; i<sizeK; i++) {
			map.put(k1Sorter.getValueAt(i), vSorter.getValueAt(i));
		}
	}	
	
	
	public final byte[] toBytesOnSortedData() throws IOException {

		if ( null == sortedList) return null;
		if ( sortedList.size() == 0 ) return null;
		
		Collection<K1> keys = new ArrayList<K1>();
		Collection<V> values = new ArrayList<V>();
		
		for (CellKeyValue<K1, V> entry : sortedList) {
			keys.add(entry.getKey());
			values.add(entry.getValue());
		}

		List<byte[]> bytesElems = new ArrayList<byte[]>();
		bytesElems.add(k1Sorter.toBytes(keys)); keys.clear();
		bytesElems.add(vSorter.toBytes(values)); values.clear();
		
		byte[] cellB = SortedBytesArray.getInstance().toBytes(bytesElems);
		bytesElems.clear();
		return cellB;
	}
	
	public final byte[] toBytesOnSortedData(final Map<K1, V> customMap) throws IOException {

		if ( null == customMap) return null;
		if ( customMap.size() == 0 ) return null;
		
		Collection<K1> keys = customMap.keySet();
		Collection<V> values = customMap.values();
		
		List<byte[]> bytesElems = new ArrayList<byte[]>();
		bytesElems.add(k1Sorter.toBytes(keys)); 
		bytesElems.add(vSorter.toBytes(values));
		
		byte[] cellB = SortedBytesArray.getInstance().toBytes(bytesElems);
		bytesElems.clear();
		return cellB;
	}	
	
	public final byte[] toBytes(final V minValue, final V maximumValue, final boolean leftInclusize, final boolean rightInclusize, final  Comparator<V> vComp) throws IOException {
		
		if ( sortedList.size() == 0 ) return null;
		Collection<K1> keys = new ArrayList<K1>();
		Collection<V> values = new ArrayList<V>();
		
		for (CellKeyValue<K1, V> entry : sortedList) {
			if ( leftInclusize ) if ( vComp.compare(entry.getValue(), minValue) < 0 ) continue;
			else if ( vComp.compare(entry.getValue(), minValue) <= 0 ) continue;
			
			if ( rightInclusize ) if ( vComp.compare(entry.getValue(), maximumValue) >= 0 ) continue;
			else if ( vComp.compare(entry.getValue(), maximumValue) > 0 ) continue;
						
			keys.add(entry.getKey());
			values.add(entry.getValue());
		}
		
		if ( keys.size() == 0 ) return null;

		
		List<byte[]> bytesElems = ObjectFactory.getInstance().getByteArrList();
		bytesElems.add(k1Sorter.toBytes(keys));
		keys.clear();
		bytesElems.add(vSorter.toBytes(values));
		values.clear();
		
		byte[] cellB = SortedBytesArray.getInstance().toBytes(bytesElems);
		
		ObjectFactory.getInstance().putByteArrList(bytesElems);
		
		return cellB;
	}		
	
	public final Collection<Integer> indexOf(final V exactValue) throws IOException {
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(exactValue, null, null, foundPositions);
		return foundPositions;
	}
	
	public final Collection<Integer> indexOf(final V minimumValue, final V maximumValue) throws IOException {
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(null, minimumValue, maximumValue, foundPositions);
		return foundPositions;
	}

	public final Set<K1> keySet(final V exactValue) throws IOException {
		Set<K1> keys = new HashSet<K1>();
		keySet(exactValue, null, null, keys);
		return keys;
	}
	
	public final void keySet(final V exactValue, final Collection<K1> keys) throws IOException {
		keySet(exactValue, null, null, keys);
	}

	public final Set<K1> keySet(final V minimumValue, final V maximumValue) throws IOException {
		Set<K1> keys = new HashSet<K1>();
		keySet(minimumValue, maximumValue, keys);
		return keys;
	}
	
	public final void keySet(final V minimumValue, final V maximumValue, final Collection<K1> keys) throws IOException {
		keySet(null, minimumValue, maximumValue, keys);
	}
	
	private final void keySet( final V exactValue, final V minimumValue, final V maximumValue, final Collection<K1> foundKeys) throws IOException {
		byte[] allKeysB = SortedBytesArray.getInstance().parse(data.data, data.offset, data.length).getValueAt(0);
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);
		
		k1Sorter.parse(allKeysB);
		for (int position : foundPositions) {
			foundKeys.add( k1Sorter.getValueAt(position) );
		}
	}
	
	public final Collection<V> values(final V exactValue) throws IOException {
		Collection<V> values = new ArrayList<V>();
		matchValues(exactValue, null, null, values);
		return values;
	}
	
	public final Collection<V> values(final V minimumValue, final V maximumValue) throws IOException {
		Collection<V> values = new ArrayList<V>();
		matchValues(null, minimumValue, maximumValue, values);
		return values;
	}

	public final void values(final V exactValue, final Collection<V> foundValues) throws IOException {
		matchValues(exactValue, null, null, foundValues);
	}
	
	public final void values(final V minimumValue, final V maximumValue, final Collection<V> foundValues) throws IOException {
		matchValues(null, minimumValue, maximumValue, foundValues);
	}	
	
	private final void matchValues(final  V exactValue, final V minimumValue,final  V maximumValue, final Collection<V> foundValues) throws IOException {

		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		
		List<Integer> foundPositions = ObjectFactory.getInstance().getIntegerList();

		byte[] allValuesB = findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);

		vSorter.parse(allValuesB);
		for (int position : foundPositions) {
			foundValues.add( vSorter.getValueAt(position) );
		}
		
		ObjectFactory.getInstance().putIntegerList(foundPositions);
		
	}

	public final Collection<V> values() throws IOException {

		Collection<V> values = new ArrayList<V>();
		values(values);
		return values;
	}
	
	public final void values( final Collection<V> values) throws IOException {
		
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}

		SortedBytesArray kvbytesA = SortedBytesArray.getInstanceArr();  
		kvbytesA.parse(data.data, data.offset, data.length);

		Reference valRef = new Reference();
		kvbytesA.getValueAtReference(1, valRef);
		vSorter.parse(data.data, valRef.offset, valRef.length);

		int size = vSorter.getSize();
		for ( int i=0; i<size; i++) {
			values.add(vSorter.getValueAt(i));
		}
	}
		
	public final Collection<V> valuesAt(final Collection<Integer> foundPositions) throws IOException {
		List<V> foundValues = new ArrayList<V>();
		valuesAt(foundValues, foundPositions );
		return foundValues;
	}

	public final void valuesAt(final Collection<V> foundValues, final Collection<Integer> foundPositions) throws IOException {
		
		byte[] allValuesB = SortedBytesArray.getInstance().parse(data.data, data.offset, data.length).getValueAt(1);
		for (int position : foundPositions) {
			foundValues.add( vSorter.parse(allValuesB).getValueAt(position));
		}
	}
	
	private final byte[] findMatchingPositions( final V exactValue, final V minimumValue, final V maximumValue, final Collection<Integer> foundPositions) throws IOException {
			
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return null;
		}

		byte[] allValsB = SortedBytesArray.getInstance().parse(data.data, data.offset, data.length).getValueAt(1);
		if ( null == allValsB) return null;
		vSorter.parse(allValsB);	
		findMatchingPositionsVsorterInitialized (exactValue, minimumValue, maximumValue, foundPositions);
		return allValsB;
	}
	
	private final void findMatchingPositionsVsorterInitialized( final V exactValue, final V minimumValue, 
		final V maximumValue, final Collection<Integer> foundPositions) throws IOException {
		
		if ( null != exactValue || null != minimumValue || null != maximumValue ) {
				
			if ( null != exactValue ) {
				vSorter.getEqualToIndexes(exactValue, foundPositions);
			} else {
				if ( null != minimumValue && null != maximumValue ) {
					vSorter.getRangeIndexesInclusive(minimumValue, maximumValue, foundPositions);
				} else if ( null != minimumValue) {
					vSorter.getGreaterThanEqualToIndexes(minimumValue, foundPositions);
				} else {
					vSorter.getLessThanEqualToIndexes(maximumValue, foundPositions);
				}
			}
		}
	}	
	
	private final void findNonMatchingPositionsVsorterInitialized( final V exactValue,
		final Collection<Integer> foundPositions) throws IOException {
			
		if ( null != exactValue) {
			vSorter.getNotEqualToIndexes(exactValue, foundPositions);
		}
	}

	public final Collection<K1> keySet() throws IOException {
		List<K1> keys = new ArrayList<K1>();
		keySet(keys);
		return keys;
	}
	
	public final void keySet( final Collection<K1> keys) throws IOException {
		
		byte[] allKeysB = SortedBytesArray.getInstance().parse(
			data.data, data.offset, data.length).getValueAt(0);
		if ( null == allKeysB ) return;
		
		int size = k1Sorter.parse(allKeysB).getSize();
		for ( int i=0; i<size; i++) {
			keys.add(k1Sorter.getValueAt(i));
		}
	}
	
	public final void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new ArrayList<CellKeyValue<K1, V>>();
		else this.sortedList.clear();
		
		List<K1> allKeys = new ArrayList<K1>();
		keySet(allKeys);
		
		List<V> allValues = new ArrayList<V>(); 
		values(allValues);
		
		int allKeysT = allKeys.size();
		if ( allKeysT != allValues.size() ) throw new IOException( 
			"Keys and Values tally mismatched : keys(" + allKeysT + ") , values(" + allValues.size() + ")");
		
		for ( int i=0; i<allKeysT; i++) {
			sortedList.add( new CellKeyValue<K1, V>(allKeys.get(i), allValues.get(i)));
		}
	}
	
	public final void remove(final K1 key) {
		if ( null == this.sortedList) return;
		int elemIndex = this.sortedList.indexOf(key);
		if ( -1 == elemIndex) return;
		this.sortedList.remove(elemIndex);
	}
	
	@Override
	public String toString() {
		if ( null == sortedList) try {parseElements();} catch (Exception e) {return e.getMessage();};
		return sortedList.toString();
	}
}
