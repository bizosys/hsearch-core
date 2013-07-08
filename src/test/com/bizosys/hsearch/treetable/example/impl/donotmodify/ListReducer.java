package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;
import java.util.Collection;

import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.functions.StatementWithOutput;

public class ListReducer implements HSearchReducer {

    @Override
    public final void appendCols(final StatementWithOutput[] queryOutput, final Collection<byte[]> mergedQueryOutput) throws IOException {
    	for (byte[] bs : mergedQueryOutput) {
    		mergedQueryOutput.add(bs);
		}
    	
    }

    @Override
    public final void appendRows(final Collection<byte[]> mergedB, final Collection<byte[]> appendB) {
    	mergedB.addAll(appendB);
    }

    @Override
    public final void appendRows(final Collection<byte[]> mergedRows, final byte[] appendRowId, final Collection<byte[]> appendRows) {
        appendRows(mergedRows, appendRows);

    }
}
