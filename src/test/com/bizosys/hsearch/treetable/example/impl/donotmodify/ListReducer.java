package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.functions.StatementWithOutput;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell2Visitor;

public class ListReducer implements HSearchReducer {

    @Override
    public final void appendQueries( final Collection<byte[]> mergedQueryOutput, final StatementWithOutput[] queryOutput) throws IOException {
    	final BitSet uniqueBits = new BitSet();
    	final StringBuilder sb = new StringBuilder();
    	
    	Cell2Visitor<Integer, String> visitor = new Cell2Visitor<Integer, String>() {
			@Override
			public void visit(Integer k, String v) {
				if(!uniqueBits.get(k)){
					uniqueBits.set(k);
					sb.append(v);
				}
			}
		};
		
    	for (StatementWithOutput output : queryOutput) {
    		
    		byte[] data = null;
    		
    		if(output.cells.iterator().hasNext())
    			data = output.cells.iterator().next();
    		
    		int size = (null == data) ? 0 : data.length;
    		if(0 == size)
    			continue;
    		
    		Cell2<Integer, String> cellData = new Cell2<Integer, String>(SortedBytesInteger.getInstance(),SortedBytesString.getInstance(), data);
    		cellData.process(visitor);
		}

    	mergedQueryOutput.add(sb.toString().getBytes());
    }

    @Override
    public final void appendRows(final byte[] appendRowId, final Collection<byte[]> mergedRows, final Collection<byte[]> appendRows) {
    	mergedRows.addAll(appendRows);
    }
}
