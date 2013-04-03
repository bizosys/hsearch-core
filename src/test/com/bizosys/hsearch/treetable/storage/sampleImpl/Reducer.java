package com.bizosys.hsearch.treetable.storage.sampleImpl;

import java.util.Collection;

import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.functions.StatementWithOutput;

public class Reducer implements HSearchReducer {

    @Override
    public void appendCols(StatementWithOutput[] queryOutputs, Collection<byte[]> mergedB) {

    	for (StatementWithOutput output : queryOutputs) {
    		if (null == output) continue;
    		if (null == output.cells) continue;
            if (output.cells.size() == 0) return;
            
            for (byte[] bs : output.cells) {
            	if ( null == bs) continue;
            	mergedB.add(bs);
			}
            System.out.println(output);
		}
    	
    }

    @Override
    public void appendRows(Collection<byte[]> mergedB, Collection<byte[]> appendB) {
        if (null == appendB) return;

    	System.out.println("appendRows : " + mergedB.size() + ":" + appendB.size());
        if (appendB.size() == 0) return;

        mergedB.addAll(appendB);
    }

    @Override
    public void appendRows(Collection<byte[]> mergedRows, byte[] appendRowId, Collection<byte[]> appendRows) {
        appendRows(mergedRows, appendRows);
    }
}
