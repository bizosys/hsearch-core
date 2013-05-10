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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.hbase.ObjectFactory;
public final class Cell11< K1, K2, K3, K4, K5, K6, K7, K8, K9, K10,V> extends CellBase<K1> {
	public ISortedByte<K2> k2Sorter = null;
	public ISortedByte<K3> k3Sorter = null;
	public ISortedByte<K4> k4Sorter = null;
	public ISortedByte<K5> k5Sorter = null;
	public ISortedByte<K6> k6Sorter = null;
	public ISortedByte<K7> k7Sorter = null;
	public ISortedByte<K8> k8Sorter = null;
	public ISortedByte<K9> k9Sorter = null;
	public ISortedByte<K10> k10Sorter = null;
	
	public ISortedByte<V> vSorter = null;
	
	public Map<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> sortedList;
	public Cell11 (  final ISortedByte<K1> k1Sorter, final ISortedByte<K2> k2Sorter, final ISortedByte<K3> k3Sorter, final ISortedByte<K4> k4Sorter, final ISortedByte<K5> k5Sorter, final ISortedByte<K6> k6Sorter, final ISortedByte<K7> k7Sorter, final ISortedByte<K8> k8Sorter, final ISortedByte<K9> k9Sorter, final ISortedByte<K10> k10Sorter, final ISortedByte<V> vSorter) {
		this.k1Sorter = k1Sorter;
		this.k2Sorter = k2Sorter;
		this.k3Sorter = k3Sorter;
		this.k4Sorter = k4Sorter;
		this.k5Sorter = k5Sorter;
		this.k6Sorter = k6Sorter;
		this.k7Sorter = k7Sorter;
		this.k8Sorter = k8Sorter;
		this.k9Sorter = k9Sorter;
		this.k10Sorter = k10Sorter;
		
		this.vSorter = vSorter;
	}
	
	public Cell11 (  final ISortedByte<K1> k1Sorter, final ISortedByte<K2> k2Sorter, final ISortedByte<K3> k3Sorter, final ISortedByte<K4> k4Sorter, final ISortedByte<K5> k5Sorter, final ISortedByte<K6> k6Sorter, final ISortedByte<K7> k7Sorter, final ISortedByte<K8> k8Sorter, final ISortedByte<K9> k9Sorter, final ISortedByte<K10> k10Sorter, 
			final ISortedByte<V> vSorter, final Map<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> sortedList ) {
		this(k1Sorter,k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,k9Sorter,k10Sorter,vSorter);
		this.sortedList = sortedList;
	}
	public Cell11 (  final ISortedByte<K1> k1Sorter, final ISortedByte<K2> k2Sorter, final ISortedByte<K3> k3Sorter, final ISortedByte<K4> k4Sorter, final ISortedByte<K5> k5Sorter, final ISortedByte<K6> k6Sorter, final ISortedByte<K7> k7Sorter, final ISortedByte<K8> k8Sorter, final ISortedByte<K9> k9Sorter, final ISortedByte<K10> k10Sorter,
			final ISortedByte<V> vSorter, final byte[] data ) {
		this(k1Sorter,k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,k9Sorter,k10Sorter,vSorter);
		int len = ( null == data) ? 0 : data.length;
		this.data = new BytesSection(data, 0, len);
	}
	
	public Cell11 (  final ISortedByte<K1> k1Sorter, final ISortedByte<K2> k2Sorter, final ISortedByte<K3> k3Sorter, final ISortedByte<K4> k4Sorter, final ISortedByte<K5> k5Sorter, final ISortedByte<K6> k6Sorter, final ISortedByte<K7> k7Sorter, final ISortedByte<K8> k8Sorter, final ISortedByte<K9> k9Sorter, final ISortedByte<K10> k10Sorter,
			final ISortedByte<V> vSorter, final BytesSection data ) {
		this(k1Sorter,k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,k9Sorter,k10Sorter,vSorter);
		this.data = data;
	}
	//Builder
	public final void put(final K1 k1,final K2 k2,final K3 k3,final K4 k4,final K5 k5,final K6 k6,final K7 k7,final K8 k8,final K9 k9,final K10 k10, final V v) {
		if ( null == sortedList) sortedList = new TreeMap<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V> >();
		
		Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V> val = null;
		if ( sortedList.containsKey(k1)) val = sortedList.get(k1);
		else {
			val = new Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>(k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,k9Sorter,k10Sorter, vSorter);
			sortedList.put(k1, val);
		}
		val.put(k2,k3,k4,k5,k6,k7,k8,k9,k10, v);
	}
	
	public final void sort(final Comparator<CellKeyValue<K10, V>> comp) {
		if ( null == sortedList) return;
		for (Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V> entry : sortedList.values()) {
			entry.sort(comp);
		}
	}	
	
	public final byte[] toBytes(final Comparator<CellKeyValue<K10, V>> comp) throws IOException {
		this.sort(comp);
		return toBytes();
	}	
	
	public final byte[] toBytes(final V minValue, final V maximumValue, final boolean leftInclusize, final boolean rightInclusize, final Comparator<V> vComp) throws IOException {
		
		List<K1> keysL = new ArrayList<K1>(1);
		List<byte[]> valuesL = new ArrayList<byte[]>(1);
		for (K1 k : this.getMap().keySet()) {
			byte[] valueB = this.getMap().get(k).toBytes(minValue, maximumValue, leftInclusize, rightInclusize, vComp);
			if ( null == valueB) continue;
			keysL.add(k);
			valuesL.add(valueB);
		}
		
		if (keysL.size() == 0 ) return null;
		
		byte[] cellB =  serializeKV(k1Sorter.toBytes(keysL) , SortedBytesArray.getInstance().toBytes(valuesL));
		keysL.clear();
		valuesL.clear();
		return cellB;
	}		
	public final Map<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> getMap(final byte[] data) throws IOException {
		if ( null == data) return null;
		this.data = new BytesSection(data, 0, data.length);
		parseElements();
		return sortedList;
	}	
	
	public final Map<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> getMap() throws IOException {
		if ( null != sortedList) return sortedList;
		if ( null != this.data) {
			parseElements();
			return sortedList;
		}
		throw new IOException("Cell is not initialized");
	}
	
	public final void getMap(final K1 exactValue, final K1 minimumValue, final K1 maximumValue, final Map<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> rows) throws IOException 
	{
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		
		Reference keyRef = new Reference();
		Reference valRef = new Reference();
		SortedBytesArray.getKeyValueAtReference(keyRef, valRef, data.data, data.offset, data.length);
		ISortedByte<byte[]> valSorter = SortedBytesArray.getInstance();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		
		Callback callback = new Callback(rows, valSorter);
		findMatchingPositions(exactValue, minimumValue, maximumValue, callback);
	}
	
	public final void getNotMap(final K1 exactValue, final Map<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> rows) throws IOException 
	{
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		
		Reference keyRef = new Reference();
		Reference valRef = new Reference();
		SortedBytesArray.getKeyValueAtReference(keyRef, valRef, data.data, data.offset, data.length);
		
		ISortedByte<byte[]> valSorter = SortedBytesArray.getInstance();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		
		Callback callback = new Callback(rows, valSorter);
		findNotMatchingPositions(exactValue, callback);
	}				
	
	public final void getInMap(final K1[] inValues, final Map<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> rows) throws IOException 
	{
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		
		Reference keyRef = new Reference();
		Reference valRef = new Reference();
		SortedBytesArray.getKeyValueAtReference(keyRef, valRef, data.data, data.offset, data.length);
		
		ISortedByte<byte[]> valSorter = SortedBytesArray.getInstance();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		
		Callback callback = new Callback(rows, valSorter);
		int size = inValues.length;	
		if ( 0 !=  size) {
			findInMatchingPositions(inValues, callback);
		}
		else {
			throw new IOException("Size for the in elemnts are zero.");
		}
	}
	/**
	 * Find matching exact value
	 * @param exactValue
	 * @return
	 * @throws IOException
	 */
	public final Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> values(final K1 exactValue) throws IOException {
		Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> values = new ArrayList<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>>();
		values(exactValue, null, null, values);
		return values;
	}
	public final Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> values(final K1 minimumValue, final K1 maximumValue) throws IOException {
		Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> values = new ArrayList<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>>();
		values(null, minimumValue, maximumValue, values);
		return values;
	}	
	
	public final void values(final K1 exactValue, final Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> foundValues) throws IOException {
		values(exactValue, null, null, foundValues);
	}
	
	public final void values(final K1 minimumValue, final K1 maximumValue, final Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> foundValues) throws IOException {
		values(null, minimumValue, maximumValue, foundValues);
	}
	
	private final void values(final K1 exactValue, final K1 minimumValue, final K1 maximumValue, 
			final Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> foundValues) throws IOException {
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		
		List<Integer> foundPositions = ObjectFactory.getInstance().getIntegerList();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		Reference valRef = new Reference();
		kvbytesA.getValueAtReference(1, valRef);
		
		SortedBytesArray valSorter = SortedBytesArray.getInstanceArr();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		Reference ref = new Reference();
		for (int position : foundPositions) {
			valSorter.getValueAtReference(position, ref);
			BytesSection sec = new BytesSection(data.data, ref.offset, ref.length);
			foundValues.add( new Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>(
				k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,k9Sorter,k10Sorter, vSorter, sec));
		}
		ObjectFactory.getInstance().putIntegerList(foundPositions);
	}
	
	public final Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> values() throws IOException {
		Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> values = new ArrayList<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>>();
		values(values);
		return values;
	}	
	
	public final void values(final Collection<Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> values) throws IOException {
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		Reference valRef = new Reference();
		kvbytesA.getValueAtReference(1, valRef);
		SortedBytesArray valSorter = SortedBytesArray.getInstanceArr();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		int size = valSorter.getSize();
		Reference ref = new Reference();
		for ( int i=0; i<size; i++) {
			valSorter.getValueAtReference(i, ref);
			BytesSection sec = new BytesSection(data.data, ref.offset, ref.length);
			values.add( new Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>( k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,k9Sorter,k10Sorter, vSorter, sec) );
		}
	}
	
	public final void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new TreeMap<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>>();
		else this.sortedList.clear();
		
		Reference keyRef = new Reference();
		Reference valRef = new Reference();
		SortedBytesArray.getKeyValueAtReference(keyRef, valRef, data.data, data.offset, data.length);
		k1Sorter.parse(data.data, keyRef.offset, keyRef.length);
		int sizeK = k1Sorter.getSize();
		SortedBytesArray valSorter = SortedBytesArray.getInstanceArr();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		int sizeV = valSorter.getSize();
		if ( sizeK != sizeV ) {
			throw new IOException(
				"Keys and Values tally mismatched : keys(" + sizeK + 
				") , values(" + sizeV + ")");					
		}
		
		Reference ref = new Reference();
		for ( int i=0; i<sizeK; i++) {
			valSorter.getValueAtReference(i, ref);
			BytesSection sec = new BytesSection(data.data, ref.offset, ref.length);
			sortedList.put(k1Sorter.getValueAt(i), new Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>( k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,k9Sorter,k10Sorter, vSorter, sec));
		}
	}
	
	@Override
	protected final List<byte[]> getEmbeddedCellBytes() throws IOException {
		List<byte[]> values = new ArrayList<byte[]>();
		for (Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V> cell10 : sortedList.values()) {
			values.add(cell10.toBytes());
		}
		return values;
	}
	
	@Override
	protected final byte[] getKeyBytes() throws IOException {
		if ( null == sortedList) throw new IOException("Cell is not initialized");
		return k1Sorter.toBytes(sortedList.keySet());
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public final void valuesUnchecked(final K1 exactValue, final K1 minimumValue,
		final K1 maximumValue, final Collection foundValues) throws IOException {
		this.values(exactValue, minimumValue, maximumValue, foundValues );
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public final void valuesUnchecked(final Collection foundValues) throws IOException {
		this.values(foundValues );
	}
	
	
	public final class Callback extends EmptyList {
		
		public ISortedByte<byte[]> valSorter;
		Map<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> rows;
		
		public Callback(final Map<K1, Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>> rows, final ISortedByte<byte[]> valSorter ) {
			this.rows = rows;
			this.valSorter = valSorter;
		}
		
		@Override
		public final boolean add(final Integer position) {
			SortedBytesArray sortedArr = (SortedBytesArray) valSorter;
			Reference valueSectionPoints = new Reference();
			sortedArr.getValueAtReference(position, valueSectionPoints);
			BytesSection valueSection = new BytesSection(
				data.data, valueSectionPoints.offset, valueSectionPoints.length);
			
			Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V> cell = new Cell10< K2, K3, K4, K5, K6, K7, K8, K9, K10,V>(
				k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,k9Sorter,k10Sorter, vSorter, valueSection);
			
			rows.put( k1Sorter.getValueAt(position), cell);
			return true;
		}
	};
	
	@Override
	public String toString() {
		if ( null == sortedList) try {parseElements();} catch (Exception e) {return e.getMessage();};
		return sortedList.toString();
	}
	
	
}
