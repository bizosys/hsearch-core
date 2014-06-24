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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.vs.PositionalBytesBase;
import com.bizosys.hsearch.hbase.ObjectFactory;

public class CellV2<V> {
	
	public ISortedByte<V> vSorter = null;
	public List<V> sortedList = null;
	public BytesSection data = null;
		
	public CellV2(final ISortedByte<V> vSorter) {
		this.vSorter = vSorter;
	}

	public CellV2(final ISortedByte<V> vSorter, final List<V> sortedList) {
		this(vSorter);
		this.sortedList = sortedList;
	}

	
	public CellV2 (final ISortedByte<V> vSorter, final byte[] data) {
		this(vSorter);
		int dataLen = ( null == data) ? 0 : data.length;
		this.data = new BytesSection(data, 0, dataLen);
	}
	
	public CellV2 (final ISortedByte<V> vSorter, final BytesSection sectionData) {
		this(vSorter);
		this.data = sectionData;
	}

	public final void add(final Integer k, final V v) {
		if ( null == sortedList) sortedList = new ArrayList<V>();
		
		int size = sortedList.size();
		int askedSize = k.intValue();
		if ( askedSize == size) {
			sortedList.add(k,v );
		} else if (askedSize < size) { //13 < 10
			sortedList.set(k, v);
		} else {
			//Iterate and fill the blanks with default value
			for ( int i=size; i<askedSize; i++) {
				sortedList.add(i, null);
			}
			sortedList.add(k,v );
		}
	}
	
	public final void sort() {
	}
	
	/**
	 * Visits all data
	 * @param visitor
	 * @throws IOException
	 */
	public final void process(final Cell2Visitor<Integer,V> visitor) throws IOException{
		
		PositionalBytesBase<V>  baseSorter = (PositionalBytesBase<V>) vSorter.parse(data.data, data.offset, data.length);
		int sizeV = baseSorter.getSize();
		V temp = null;

		for ( int i=0; i<sizeV; i++) {

			temp = vSorter.getValueAt(i);
			if(baseSorter.isEqual(baseSorter.defaultValue, temp))
				continue;
			
			visitor.visit(i, temp);
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
	public final void process(final V exactValue, final V minimumValue, final V maximumValue, final Cell2Visitor<Integer,V> visitor) throws IOException {
	
		PositionalBytesBase<V>  baseSorter = (PositionalBytesBase<V>) vSorter.parse(data.data, data.offset, data.length);
		int sizeV = baseSorter.getSize();

		if ( null != exactValue || null != minimumValue || null != maximumValue) {
			findMatchingPositionsVsorterInitialized(
					exactValue, minimumValue, maximumValue, new CellV2FoundIndex<V>(vSorter, visitor));
		} else {
			V temp = null;

			for ( int i=0; i<sizeV; i++) {

				temp = vSorter.getValueAt(i);
				if(baseSorter.isEqual(baseSorter.defaultValue, temp))
					continue;
				
				visitor.visit(i, temp);
			}		
		}
	}	
	
	public final void processNot(final V exactValue, final Cell2Visitor<Integer,V> visitor) throws IOException {
		if ( null != exactValue ) {
			findNonMatchingPositionsVsorterInitialized( exactValue, 
				new CellV2FoundIndex<V>(vSorter, visitor) );
		} else {
			throw new IOException("Not queries are not yet supported for ranges.");
		}		
	}
	
	
	public final List<V> getMap(final byte[] data) throws IOException {
		int dataLen = ( null == data) ? 0 : data.length;
		this.data = new BytesSection(data, 0, dataLen) ;
		parseElements();
		return sortedList;
	}
	
	public final List<V> getMap() throws IOException {
		if ( null != sortedList) return sortedList;
		if ( null != this.data) {
			parseElements();
			return sortedList;
		}
		throw new IOException("Cell2 is not initialized");
	}
	
	public final void getMap(final List<V> valueContainer) throws IOException {
		if ( null != sortedList) {
			valueContainer.addAll(sortedList);
			return;
		}
		
		this.sortedList = valueContainer;
		if ( null != this.data) {
			parseElements();
			return;
		}
		throw new IOException("Cell2 is not initialized");
	}	
	
	public final void getMap(final List<Integer> kContainer, final List<V> vContainer) throws IOException{
		keySet(kContainer);
		values(vContainer);
	}
	
	public final void getMap(final V exactValue, final V minimumValue, final V maximumValue, 
			final List<Integer> reusableFoundPosArray, final List<Integer> kContainer, final List<V> vContainer) throws IOException {
		
		List<Integer> foundPositions = reusableFoundPosArray;
		byte[] allValsB = findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);
		
		PositionalBytesBase<V>  baseSorter = (PositionalBytesBase<V>) vSorter.parse(allValsB);
		V temp = null;
		for (int position : foundPositions) {
			temp = baseSorter.getValueAt(position);
			if(baseSorter.isEqual(baseSorter.defaultValue, temp))
				continue;
			
			kContainer.add(position);
			vContainer.add(temp);
		}
	}		
	
	public final void populate(final Map<Integer,V> map) throws IOException {
		PositionalBytesBase<V>  baseSorter = (PositionalBytesBase<V>) vSorter.parse(data.data, data.offset, data.length);
		int sizeV = baseSorter.getSize();
		V temp = null;
		
		for ( int i=0; i<sizeV; i++) {
			temp = baseSorter.getValueAt(i);
			if(baseSorter.isEqual(baseSorter.defaultValue, temp))
				continue;
			map.put(i, vSorter.getValueAt(i));
		}
	}	
	
	
	public final byte[] toBytes() throws IOException {

		if ( null == sortedList) return null;
		if ( sortedList.size() == 0 ) return null;
		return vSorter.toBytes(this.sortedList);
	}
		
	public final byte[] toBytes(final V minValue, final V maximumValue, final boolean leftInclusize, final boolean rightInclusize, final  Comparator<V> vComp) throws IOException {
		
		if ( sortedList.size() == 0 ) return null;
		
		List<V> values = new ArrayList<V>();
		int i=0;
		for (V entry : sortedList) {
			i++;
			if ( leftInclusize ) if ( vComp.compare(entry, minValue) < 0 ) continue;
			else if ( vComp.compare(entry, minValue) <= 0 ) continue;
			
			if ( rightInclusize ) if ( vComp.compare(entry, maximumValue) >= 0 ) continue;
			else if ( vComp.compare(entry, maximumValue) > 0 ) continue;
						
			values.add(i,entry);
		}
		
		if ( i == 0 ) return null;

		byte[] cellB = vSorter.toBytes(values);
		values.clear();
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

	public final Set<Integer> keySet(final V exactValue) throws IOException {
		Set<Integer> keys = new HashSet<Integer>();
		keySet(exactValue, null, null, keys);
		return keys;
	}
	
	public final void keySet(final V exactValue, final Collection<Integer> keys) throws IOException {
		keySet(exactValue, null, null, keys);
	}

	public final Set<Integer> keySet(final V minimumValue, final V maximumValue) throws IOException {
		Set<Integer> keys = new HashSet<Integer>();
		keySet(minimumValue, maximumValue, keys);
		return keys;
	}
	
	public final void keySet(final V minimumValue, final V maximumValue, final Collection<Integer> keys) throws IOException {
		keySet(null, minimumValue, maximumValue, keys);
	}
	
	private final void keySet( final V exactValue, final V minimumValue, final V maximumValue, final Collection<Integer> foundKeys) throws IOException {
		
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundKeys);
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

		PositionalBytesBase<V>  baseSorter = (PositionalBytesBase<V>) vSorter.parse(data.data, data.offset, data.length);
		V temp = null;
		int size = baseSorter.getSize();
		for ( int i=0; i < size; i++) {

			temp = vSorter.getValueAt(i);
			if(baseSorter.isEqual(baseSorter.defaultValue, temp))
				continue;
			
			values.add(vSorter.getValueAt(i));
		}
	}
		
	public final Collection<V> valuesAt(final Collection<Integer> foundPositions) throws IOException {
		List<V> foundValues = new ArrayList<V>();
		valuesAt(foundValues, foundPositions );
		return foundValues;
	}

	public final void valuesAt(final Collection<V> foundValues, final Collection<Integer> foundPositions) throws IOException {
		
		if ( null == data.data) return;
		PositionalBytesBase<V>  baseSorter = (PositionalBytesBase<V>) vSorter.parse(data.data, data.offset, data.length);
		V temp = null;
		for (int position : foundPositions) {

			temp = vSorter.getValueAt(position);
			if(baseSorter.isEqual(baseSorter.defaultValue, temp))
				continue;

			foundValues.add( vSorter.getValueAt(position));
		}
	}
	
	private final byte[] findMatchingPositions( final V exactValue, final V minimumValue, final V maximumValue, final Collection<Integer> foundPositions) throws IOException {
			
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return null;
		}

		if ( null == data.data) return null;
		vSorter.parse(data.data, data.offset, data.length);	
		findMatchingPositionsVsorterInitialized (exactValue, minimumValue, maximumValue, foundPositions);
		return data.data;
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
	
	public final void processIn(final V[] inValues, final Cell2Visitor<Integer,V> visitor) throws IOException {

		if ( null == this.data ) return;
		if ( null == this.data.data ) return;
		
		PositionalBytesBase<V>  baseSorter = (PositionalBytesBase<V>) vSorter.parse(data.data, data.offset, data.length);
		int size = baseSorter.getSize();
		if ( 0 !=  size) {
			findInMatchingPositionsVsorterInitialized( inValues, 
				new CellV2FoundIndex<V>(vSorter, visitor) );
		} else {
			throw new IOException("Size for the in elemnts are zero.");
		}		
		
	}

	private final void findInMatchingPositionsVsorterInitialized( final V[] inValues,
		final Collection<Integer> foundPositions) throws IOException {
		int size = inValues.length;	
		if ( 0 !=  size) {
			for(int i = 0; i < size; i++){
				vSorter.getEqualToIndexes(inValues[i], foundPositions);
			}
		}
	}

	public final Collection<Integer> keySet() throws IOException {
		List<Integer> keys = new ArrayList<Integer>();
		keySet(keys);
		return keys;
	}
	
	public final void keySet( final Collection<Integer> keys) throws IOException {
		
		if ( null == this.data ) return;
		if ( null == this.data.data ) return;
		
		PositionalBytesBase<V>  baseSorter = (PositionalBytesBase<V>) vSorter.parse(data.data, data.offset, data.length);
		int size = baseSorter.getSize();
		for ( int i=0; i<size; i++) {
			if(baseSorter.isEqual(baseSorter.defaultValue, vSorter.getValueAt(i)))
				continue;
			keys.add(i);
		}
	}
	
	public final void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new ArrayList<V>();
		else this.sortedList.clear();
		values(this.sortedList);
	}
	
	public final void remove(final Integer key) {
		if ( null == this.sortedList) return;
		int elemIndex = this.sortedList.indexOf(key);
		if ( -1 == elemIndex) return;
		this.sortedList.remove(elemIndex);
	}
	
	public void clear() {
		if ( null != sortedList) sortedList.clear();
		this.data = null;
	}
	
	@Override
	public String toString() {
		if ( null == sortedList) try {parseElements();} catch (Exception e) {return e.getMessage();};
		return sortedList.toString();
	}
}
