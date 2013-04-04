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

public final class Cell3<K1, K2, V> extends CellBase<K1> {
	
	public ISortedByte<K2> k2Sorter = null;
	public ISortedByte<V> vSorter = null;
	
	public Map<K1, Cell2<K2, V>> sortedList;
	
	public Cell3(final ISortedByte<K1> k1Sorter, final ISortedByte<K2> k2Sorter,
			ISortedByte<V> vSorter) {
		this.k1Sorter = k1Sorter;
		this.k2Sorter = k2Sorter;
		this.vSorter = vSorter;
	}

	public Cell3 (final ISortedByte<K1> k1Sorter,final ISortedByte<K2> k2Sorter, final ISortedByte<V> vSorter, final Map<K1, Cell2<K2, V>> sortedList ) {
		this(k1Sorter, k2Sorter, vSorter);
		this.sortedList = sortedList;
	}

	public Cell3 (final ISortedByte<K1> k1Sorter,final ISortedByte<K2> k2Sorter, final ISortedByte<V> vSorter, byte[] data ) {
		this(k1Sorter, k2Sorter, vSorter);
		int len = ( null == data) ? 0 : data.length;
		this.data = new BytesSection(data, 0, len);
	}
	
	public Cell3 (final ISortedByte<K1> k1Sorter, final ISortedByte<K2> k2Sorter, final ISortedByte<V> vSorter, final BytesSection data ) {
		this(k1Sorter, k2Sorter, vSorter);
		this.data = data;
	}

	//Builder
	
	public final void put(final K1 k1, final K2 k2, final V v) {
		if ( null == sortedList) {
			sortedList = new TreeMap<K1, Cell2<K2,V>>();
		}
		
		Cell2<K2,V> val = null;
		if ( sortedList.containsKey(k1)) val = sortedList.get(k1);
		else {
			val = new Cell2<K2, V>(k2Sorter, vSorter);
			sortedList.put(k1, val);
		}

		val.add(k2, v);
		
	}
	
	public final void sort(final Comparator<CellKeyValue<K2, V>> comp) {
		if ( null == sortedList) return;
		for (Cell2<K2,V> entry : sortedList.values()) {
			entry.sort(comp);
		}
	}
	

	public final byte[] toBytes(final Comparator<CellKeyValue<K2, V>> comp) throws IOException {
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
		
		byte[] cellB = serializeKV(k1Sorter.toBytes(keysL), SortedBytesArray.getInstance().toBytes(valuesL));
		keysL.clear();
		valuesL.clear();

		return cellB;
	}		
	
	public final void getMap(final K1 exactValue, final K1 minimumValue, final K1 maximumValue, final Map<K1, Cell2<K2, V>> rows) throws IOException 
	{
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);

		Reference keyRef = new Reference();
		kvbytesA.getValueAtReference(0, keyRef);
		
		Reference valRef = new Reference();
		kvbytesA.getValueAtReference(1, valRef);
		
		ISortedByte<byte[]> valSorter = SortedBytesArray.getInstance();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		
		Callback callback = new Callback(rows, valSorter);
		findMatchingPositions(exactValue, minimumValue, maximumValue, callback);
	}
	

	public final Map<K1, Cell2<K2, V>> getMap(final byte[] data) throws IOException {
		if ( null == data) return null;
		this.data = new BytesSection(data, 0, data.length);
		parseElements();
		return sortedList;
	}	

	public final Map<K1, Cell2<K2, V>> getMap() throws IOException {
		if ( null != sortedList) return sortedList;
		if ( null != this.data) {
			parseElements();
			return sortedList;
		}
		throw new IOException("Cell is not initialized");
	}	
	
	
	public final Collection<Cell2<K2, V>> values(final K1 exactValue) throws IOException {
		Collection<Cell2<K2, V>> values = new ArrayList<Cell2<K2, V>>();
		values(exactValue, null, null, values);
		return values;
	}	
	
	public final Collection<Cell2<K2, V>> values(final K1 minimumValue, final K1 maximumValue) throws IOException {
		Collection<Cell2<K2, V>> values = new ArrayList<Cell2<K2, V>>();
		values(null, minimumValue, maximumValue, values);
		return values;
	}		
	
	public final void values(final K1 exactValue, final Collection<Cell2<K2, V>> foundValues) throws IOException {
		values(exactValue, null, null, foundValues);
	}
	
	public final void values(final K1 minimumValue, final K1 maximumValue, final Collection<Cell2<K2, V>> foundValues) throws IOException {
		values(null, minimumValue, maximumValue, foundValues);
	}
	
	private final void values(final K1 exactValue, final K1 minimumValue, final K1 maximumValue, 
			Collection<Cell2<K2, V>> foundValues) throws IOException {

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
		if ( null != valRef )  {
			SortedBytesArray valSorter = SortedBytesArray.getInstanceArr();
			valSorter.parse(data.data, valRef.offset, valRef.length);

			Reference ref = new Reference();
			for (int position : foundPositions) {
				valSorter.getValueAtReference(position, ref);
				BytesSection sec = new BytesSection(data.data, ref.offset, ref.length);
				foundValues.add( new Cell2<K2, V>(k2Sorter, vSorter, sec));
			}
		}
		ObjectFactory.getInstance().putIntegerList(foundPositions);
	}
	
	public final Collection<Cell2<K2, V>> values() throws IOException {
		Collection<Cell2<K2, V>> values = new ArrayList<Cell2<K2, V>>();
		values(values);
		return values;
	}	
	
	public final void values(final Collection<Cell2<K2, V>> values) throws IOException {
		
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
			values.add( new Cell2<K2, V>( k2Sorter, vSorter, sec) );
		}
	}
	
	public final void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new TreeMap<K1, Cell2<K2, V>>();
		else this.sortedList.clear();
		
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		
		Reference keyRef = kvbytesA.getValueAtReference(0);
		Reference valRef = kvbytesA.getValueAtReference(1);
		
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
			sortedList.put(k1Sorter.getValueAt(i), new Cell2<K2, V>( k2Sorter, vSorter, sec));
		}
	}
	
	@Override
	protected final List<byte[]> getEmbeddedCellBytes() throws IOException {
		List<byte[]> values = new ArrayList<byte[]>();
		for (Cell2<K2, V> cell2 : sortedList.values()) {
			values.add(cell2.toBytesOnSortedData());
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
	public final void valuesUnchecked(final K1 exactValue,final  K1 minimumValue, final K1 maximumValue, final Collection foundValues) throws IOException {
		this.values(exactValue, minimumValue, maximumValue, foundValues );
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public final void valuesUnchecked(final Collection foundValues) throws IOException {
		this.values(foundValues );
	}
	
	
	public final class Callback extends EmptyList {
		
		public ISortedByte<byte[]> valSorter;
		Map<K1, Cell2<K2, V>> rows;
		
		public Callback(final Map<K1, Cell2<K2, V>> rows, final ISortedByte<byte[]> valSorter ) {
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
			
			Cell2<K2, V> cell2 = new Cell2<K2, V>(k2Sorter, vSorter, valueSection);
			rows.put( k1Sorter.getValueAt(position), cell2);
			return true;
		}
	};
	
	@Override
	public String toString() {
		if ( null == sortedList) try {parseElements();} catch (Exception e) {return e.getMessage();};
		return sortedList.toString();
	}
	
}
