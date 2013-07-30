package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;
import java.util.Collection;

import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.functions.StatementWithOutput;

public class ListReducer implements HSearchReducer {

    @Override
    public final void appendQueries( final Collection<byte[]> mergedQueryOutput, final StatementWithOutput[] queryOutput) throws IOException {
    	for (byte[] bs : mergedQueryOutput) {
    		mergedQueryOutput.add(bs);
		}
    }

    @Override
    public final void appendRows(final byte[] appendRowId, final Collection<byte[]> mergedRows, final Collection<byte[]> appendRows) {
    	mergedRows.addAll(appendRows);
    }
}
