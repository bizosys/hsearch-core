package com.bizosys.hsearch.treetable.storage.sampleImpl.donotmodify;

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

import com.bizosys.hsearch.treetable.storage.sampleImpl.*;

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

public class HSearchTableExamResult implements IHSearchTable {
	
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
	public Map<String, Cell3<String, Integer, Float>> cell3L = null;

	public Cell4Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,Float matchingCell4, Float cellMin4, Float cellMax4,String matchingCell2, String cellMin2, String cellMax2,String matchingCell1, String cellMin1, String cellMax1) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell1 = matchingCell1;
		this.cellMin1 = cellMin1;
		this.cellMax1 = cellMax1;
		this.cell3L = new Cell3Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4,matchingCell2, cellMin2, cellMax2);
	}
	@Override
	public Cell4<String, String, Integer, Float> put(Integer key, Cell4<String, String, Integer, Float> value) {
	try {
		cell2Visitor.cell4Key = key;
		if (query.filterCells[1]) {
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
	public Map<String, Cell2<Integer, Float>> cell2L = null;

	public Cell3Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,Float matchingCell4, Float cellMin4, Float cellMax4,String matchingCell2, String cellMin2, String cellMax2) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell2 = matchingCell2;
		this.cellMin2 = cellMin2;
		this.cellMax2 = cellMax2;
		this.cell2L = new Cell2Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4);
	}
	@Override
	public Cell3<String, Integer, Float> put(String key, Cell3<String, Integer, Float> value) {
	try {
		cell2Visitor.cell3Key = key;
		if (query.filterCells[2]) {
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
		public Cell2FilterVisitor cell2Visitor;
		
		public Cell2Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,
			Float matchingCell4, Float cellMin4, Float cellMax4) {
			this.query = query;
			this.cell2Visitor = cell2Visitor;

			this.matchingCell4 = matchingCell4;
			this.cellMin4 = cellMin4;
			this.cellMax4 = cellMax4;
		}
		
		@Override
		public Cell2<Integer, Float> put(String key, Cell2<Integer, Float> value) {
			
			try {
				cell2Visitor.cell2Key = key;
				Cell2<Integer, Float> cell2Val = value;

				if (query.filterCells[4]) {
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

		public int cell4Key;
		public String cell3Key;
		public String cell2Key;
		
		public int mode = MODE_COLS;
        public Cell2FilterVisitor(HSearchQuery query,IHSearchPlugin plugin, PluginExamResultBase.TablePartsCallback tablePartsCallback, int mode) {
			
            this.query = query;
            this.plugin = plugin;
            this.tablePartsCallback = tablePartsCallback;
            this.mode = mode;

        }
		
		public void set(Integer matchingCell3, Integer cellMin3, Integer cellMax3) {
			this.matchingCell3 = matchingCell3;
			this.cellMin3 = cellMin3;
			this.cellMax3 = cellMax3;
		}
		
		
		
		@Override
		public final void visit(Integer cell1Key, Float cell1Val) {
			if (query.filterCells[3]) {
				if (null != matchingCell3) {
					if (matchingCell3.intValue() != cell1Key.intValue()) return;
				} else {
					if (cell1Key.intValue() < cellMin3.intValue() || cell1Key.intValue() > cellMax3.intValue()) return;
				}
			}

			if (null != plugin) {
            	switch (this.mode) {
            		case MODE_COLS :
            			tablePartsCallback.onRowCols(cell4Key, cell3Key, cell2Key, cell1Key, cell1Val);
            			break;

            		case MODE_KEY :
            			tablePartsCallback.onRowKey(cell1Key);
            			break;
            		case MODE_KEYVAL :
            			tablePartsCallback.onRowKeyValue(cell1Key, cell1Val);
            			break;
            		case MODE_VAL:
            			tablePartsCallback.onRowValue(cell1Val);
            			break;
            	}
			} 
		}
	}	
	
	///////////////////////////////////////////////////////////////////	

	Cell5<Integer,String, String, Integer, Float> table = createBlankTable();

	public HSearchTableExamResult() {
	}
	
	public Cell5<Integer,String, String, Integer, Float> createBlankTable() {
		return new Cell5<Integer,String, String, Integer, Float>
			(
				SortedBytesInteger.getInstance(),
				SortedBytesString.getInstance(),
				SortedBytesString.getInstance(),
				SortedBytesInteger.getInstance(),
				SortedBytesFloat.getInstance()
			);
	}

	public byte[] toBytes() throws IOException {
		if ( null == table) return null;
		return table.toBytes(new FloatComparator<Integer>());
	}

	public void put (Integer age, String role, String location, Integer empid, Float mark) {
		table.put( age, role, location, empid, mark );
	}
	
    @Override
    public void get(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException, NumberFormatException {
    	iterate(input, query, pluginI, MODE_COLS);
    }

    @Override
    public void keySet(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEY);
    }

    public void values(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_VAL);
    }

    public void keyValues(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEYVAL);
    }
    
    private void iterate(byte[] input, HSearchQuery query, IHSearchPlugin pluginI, int mode) throws IOException, NumberFormatException {
    	
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


		Cell4Map cell4L = new Cell4Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4,matchingCell2, cellMin2, cellMax2,matchingCell1, cellMin1, cellMax1);

        this.table.data = new BytesSection(input);
        if (query.filterCells[0]) {
            this.table.getMap(matchingCell0, cellMin0, cellMax0, cell4L);
        } else {
            this.table.sortedList = cell4L;
            this.table.parseElements();
        }

        if (null != callback) callback.onReadComplete();
        if (null != plugin) plugin.onReadComplete();
    }

    public PluginExamResultBase castPlugin(IHSearchPlugin pluginI)
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
    public void clear() throws IOException {
        table.getMap().clear();
    }

}
	