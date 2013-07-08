package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesUnsignedShort;
import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.byteutils.Storable;

import com.bizosys.hsearch.treetable.example.impl.*;

import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell3;
import com.bizosys.hsearch.treetable.Cell4;
import com.bizosys.hsearch.treetable.Cell5;

import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2Visitor;
import com.bizosys.hsearch.treetable.CellBase;

import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

import com.bizosys.hsearch.treetable.unstructured.IIndexOffsetTable;
import com.bizosys.hsearch.treetable.unstructured.IIndexFrequencyTable;
import com.bizosys.hsearch.treetable.unstructured.IIndexPositionsTable;

import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.treetable.CellComparator.BooleanComparator;
import com.bizosys.hsearch.treetable.CellComparator.ByteComparator;
import com.bizosys.hsearch.treetable.CellComparator.BytesComparator;
import com.bizosys.hsearch.treetable.CellComparator.ShortComparator;
import com.bizosys.hsearch.treetable.CellComparator.IntegerComparator;
import com.bizosys.hsearch.treetable.CellComparator.FloatComparator;
import com.bizosys.hsearch.treetable.CellComparator.LongComparator;
import com.bizosys.hsearch.treetable.CellComparator.DoubleComparator;
import com.bizosys.hsearch.treetable.CellComparator.StringComparator;
import com.bizosys.hsearch.treetable.ValueComparator;
import com.bizosys.hsearch.util.EmptyMap;

public final class HSearchTableExamResult implements IHSearchTable {
	
	public static boolean DEBUG_ENABLED = false;
	
	public static final int MODE_COLS = 0;
    public static final int MODE_KEY = 1;
    public static final int MODE_VAL = 2;
    public static final int MODE_KEYVAL = 3;
	
	
	public static final class Cell4Map
		 extends EmptyMap<Integer, Cell4<String, String, Integer, Float>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public String matchingCell1;
	public String cellMin1; 
	public String cellMax1;
	public String[] inValues1;
	public Map<String, Cell3<String, Integer, Float>> cell3L = null;

	public Cell4Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor, final Float matchingCell4, final Float cellMin4, final Float cellMax4, final Float[] inValues4, final String matchingCell2, final String cellMin2, final String cellMax2, final String[] inValues2, final String matchingCell1, final String cellMin1, final String cellMax1, final String[] inValues1) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell1 = matchingCell1;
		this.cellMin1 = cellMin1;
		this.cellMax1 = cellMax1;
		this.inValues1 = inValues1;
		this.cell3L = new Cell3Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4, inValues4,matchingCell2, cellMin2, cellMax2, inValues2);
	}
	@Override
	public final Cell4<String, String, Integer, Float> put( final Integer key, final Cell4<String, String, Integer, Float> value) {
	try {
		cell2Visitor.cell4Key = key;
		if (query.filterCells[1]) {
			if (query.notValCells[1])
				value.getNotMap(matchingCell1, cell3L);
			else if (query.inValCells[1])
				value.getInMap(inValues1, cell3L);
			else
			value.getMap(matchingCell1, cellMin1, cellMax1, cell3L);
		 } else {
			value.sortedList = cell3L;
			value.parseElements();
		}
		return value;
		} catch (IOException e) {
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}
}


public static final class Cell3Map
		 extends EmptyMap<String, Cell3<String, Integer, Float>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public String matchingCell2;
	public String cellMin2; 
	public String cellMax2;
	public String[] inValues2;
	public Map<String, Cell2<Integer, Float>> cell2L = null;

	public Cell3Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor, final Float matchingCell4, final Float cellMin4, final Float cellMax4, final Float[] inValues4, final String matchingCell2, final String cellMin2, final String cellMax2, final String[] inValues2) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell2 = matchingCell2;
		this.cellMin2 = cellMin2;
		this.cellMax2 = cellMax2;
		this.inValues2 = inValues2;
		this.cell2L = new Cell2Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4, inValues4);
	}
	@Override
	public final Cell3<String, Integer, Float> put( final String key, final Cell3<String, Integer, Float> value) {
	try {
		cell2Visitor.cell3Key = key;
		if (query.filterCells[2]) {
			if (query.notValCells[2])
				value.getNotMap(matchingCell2, cell2L);
			else if (query.inValCells[2])
				value.getInMap(inValues2, cell2L);
			else
			value.getMap(matchingCell2, cellMin2, cellMax2, cell2L);
		 } else {
			value.sortedList = cell2L;
			value.parseElements();
		}
		return value;
		} catch (IOException e) {
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}
}



	
	public static final class Cell2Map extends EmptyMap<String, Cell2<Integer, Float>> {

		public HSearchQuery query;
		public Float matchingCell4;
		public Float cellMin4;
		public Float cellMax4;
		public Float[] inValues4;
		public Cell2FilterVisitor cell2Visitor;
		
		public Cell2Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor,
			final Float matchingCell4, final Float cellMin4, final Float cellMax4, final Float[] inValues4) {
			this.query = query;
			this.cell2Visitor = cell2Visitor;

			this.matchingCell4 = matchingCell4;
			this.cellMin4 = cellMin4;
			this.cellMax4 = cellMax4;
			this.inValues4 = inValues4;
		}
		
		@Override
		public final Cell2<Integer, Float> put(final String key, final Cell2<Integer, Float> value) {
			
			try {
				cell2Visitor.cell2Key = key;
				Cell2<Integer, Float> cell2Val = value;

				if (query.filterCells[4]) {
					if(query.notValCells[4])
						cell2Val.processNot(matchingCell4, cell2Visitor);
					else if(query.inValCells[4])
						cell2Val.processIn(inValues4, cell2Visitor);
					else 
						cell2Val.process(matchingCell4, cellMin4, cellMax4,cell2Visitor);
				} else {
					cell2Val.process(cell2Visitor);
				}
				return value;
			} catch (IOException e) {
				throw new IndexOutOfBoundsException(e.getMessage());
			}
		}
	}
	
	///////////////////////////////////////////////////////////////////	
	
	public static final class Cell2FilterVisitor implements Cell2Visitor<Integer, Float> {
		public HSearchQuery query;
		public IHSearchPlugin plugin;

		public PluginExamResultBase.TablePartsCallback tablePartsCallback = null;

		public Integer matchingCell3;
		public Integer cellMin3;
		public Integer cellMax3;
		public Integer[] inValues3;

		public int cell4Key;
		public String cell3Key;
		public String cell2Key;
		
		public int mode = MODE_COLS;
        public Cell2FilterVisitor(final HSearchQuery query, final IHSearchPlugin plugin, final PluginExamResultBase.TablePartsCallback tablePartsCallback, final int mode) {
			
            this.query = query;
            this.plugin = plugin;
            this.tablePartsCallback = tablePartsCallback;
            this.mode = mode;

        }
		
		public final void set(final Integer matchingCell3, final Integer cellMin3, final Integer cellMax3, final Integer[] inValues3) {
			this.matchingCell3 = matchingCell3;
			this.cellMin3 = cellMin3;
			this.cellMax3 = cellMax3;
			this.inValues3 = inValues3;
		}
		
		
		
		@Override
		public final void visit(final Integer cell1Key, final Float cell1Val)  {

			//Is it all or not.
			if (query.filterCells[3]) {
				
				//IS it exact or not
				if (null != matchingCell3) {
					if ( query.notValCells[3] ) {
						//Exact val match
						if (matchingCell3.intValue() == cell1Key.intValue()) return;
					} else {
						//Not Exact val 
						if (matchingCell3.intValue() != cell1Key.intValue()) return;
					}
				} else {
					//Either range or IN
					if ( query.inValCells[3]) {
						//IN
						boolean isMatched = false;
						//LOOKING FOR ONE MATCHING
						for ( Object obj : query.inValuesAO[3]) {
							Integer objI = (Integer) obj;
							isMatched = cell1Key.intValue() == objI.intValue();
							
							//ONE MATCHED, NO NEED TO PROCESS
							if ( query.notValCells[3] ) { 
								if (!isMatched ) break; 
							} else {
								if (isMatched ) break;
							}
						}
						if ( !isMatched ) return; //NONE MATCHED
						
					} else {
						//RANGE
						boolean isMatched = cell1Key.intValue() < cellMin3.intValue() || 
											cell1Key.intValue() > cellMax3.intValue();
						if ( query.notValCells[3] ) {
							//Not Exact Range
							if (!isMatched ) return;
						} else {
							//Exact Range
							if (isMatched ) return;
						}
					}
				}
			}

			if (null != plugin) {
            	switch (this.mode) {
            		case MODE_COLS :
            			tablePartsCallback.map(cell4Key, cell3Key, cell2Key, cell1Key, cell1Val);
            			break;

            		case MODE_KEY :
            			tablePartsCallback.map(cell1Key);
            			break;
            		case MODE_KEYVAL :
            			tablePartsCallback.map(cell1Key, cell1Val);
            			break;
            		case MODE_VAL:
            			tablePartsCallback.map(cell1Val);
            			break;
            	}
			} 
		}
	}	
	
	///////////////////////////////////////////////////////////////////	

	Cell5<Integer,String, String, Integer, Float> table = createBlankTable();

	public HSearchTableExamResult() {
	}
	
	public final Cell5<Integer,String, String, Integer, Float> createBlankTable() {
		return new Cell5<Integer,String, String, Integer, Float>
			(
				SortedBytesInteger.getInstance(),
				SortedBytesString.getInstance(),
				SortedBytesString.getInstance(),
				SortedBytesInteger.getInstance(),
				SortedBytesFloat.getInstance()
			);
	}

	public final byte[] toBytes() throws IOException {
		if ( null == table) return null;
		return table.toBytes(new FloatComparator<Integer>());
	}

	public final void put (Integer age, String role, String location, Integer empid, Float mark) {
		table.put( age, role, location, empid, mark );
	}
	
    @Override
    public final void get(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException, NumberFormatException {
    	iterate(input, query, pluginI, MODE_COLS);
    }

    @Override
    public final void keySet(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEY);
    }

    public final void values(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_VAL);
    }

    public final void keyValues(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEYVAL);
    }
    
    private final void iterate(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI, final int mode) throws IOException, NumberFormatException {
    	
        PluginExamResultBase plugin = castPlugin(pluginI);
        PluginExamResultBase.TablePartsCallback callback = plugin.getPart();

        Cell2FilterVisitor cell2Visitor = new Cell2FilterVisitor(query, pluginI, callback, mode);

        query.parseValuesConcurrent(new String[]{"Integer", "String", "String", "Integer", "Float"});

		Integer matchingCell0 = ( query.filterCells[0] ) ? (Integer) query.exactValCellsO[0]: null;
		String matchingCell1 = ( query.filterCells[1] ) ? (String) query.exactValCellsO[1]: null;
		String matchingCell2 = ( query.filterCells[2] ) ? (String) query.exactValCellsO[2]: null;
		cell2Visitor.matchingCell3 = ( query.filterCells[3] ) ? (Integer) query.exactValCellsO[3]: null;
		Float matchingCell4 = ( query.filterCells[4] ) ? (Float) query.exactValCellsO[4]: null;


		Integer cellMin0 = ( query.minValCells[0] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[0]).intValue();
		String cellMin1 = null;
		String cellMin2 = null;
		cell2Visitor.cellMin3 = ( query.minValCells[3] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[3]).intValue();
		Float cellMin4 = ( query.minValCells[4] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[4]).floatValue();


		Integer cellMax0 =  (query.maxValCells[0] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[0]).intValue();
		String cellMax1 = null;
		String cellMax2 = null;
		cell2Visitor.cellMax3 =  (query.maxValCells[3] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[3]).intValue();
		Float cellMax4 =  (query.maxValCells[4] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[4]).floatValue();


		Integer[] inValues0 =  (query.inValCells[0]) ? (Integer[])query.inValuesAO[0]: null;
		String[] inValues1 =  (query.inValCells[1]) ? (String[])query.inValuesAO[1]: null;
		String[] inValues2 =  (query.inValCells[2]) ? (String[])query.inValuesAO[2]: null;
		cell2Visitor.inValues3 =  (query.inValCells[3]) ? (Integer[])query.inValuesAO[3]: null;
		Float[] inValues4 =  (query.inValCells[4]) ? (Float[])query.inValuesAO[4]: null;


		Cell4Map cell4L = new Cell4Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4, inValues4,matchingCell2, cellMin2, cellMax2, inValues2,matchingCell1, cellMin1, cellMax1, inValues1);

        this.table.data = new BytesSection(input);
        if (query.filterCells[0]) {
			if(query.notValCells[0])
				this.table.getNotMap(matchingCell0, cell4L);
			else if(query.inValCells[0])
				this.table.getInMap(inValues0, cell4L);
			else	
				this.table.getMap(matchingCell0, cellMin0, cellMax0, cell4L);
        } else {
            this.table.sortedList = cell4L;
            this.table.parseElements();
        }

        if (null != callback) callback.close();
        if (null != plugin) plugin.onReadComplete();
    }

    public final PluginExamResultBase castPlugin(final IHSearchPlugin pluginI)
            throws IOException {
        PluginExamResultBase plugin = null;
        if (null != pluginI) {
            if (pluginI instanceof PluginExamResultBase) {
                plugin = (PluginExamResultBase) pluginI;
            }
            if (null == plugin) {
                throw new IOException("Invalid plugin Type :" + pluginI);
            }
        }
        return plugin;
    }

    /**
     * Free the cube data
     */
    public final void clear() throws IOException {
        if ( null !=  table.sortedList) table.sortedList.clear();
    }

}
	