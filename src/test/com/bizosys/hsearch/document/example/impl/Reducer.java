package com.bizosys.hsearch.document.example.impl;

import java.io.IOException;
import java.util.Collection;

import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.functions.StatementWithOutput;

public class Reducer implements HSearchReducer {

    @Override
    public final void appendQueries(final Collection<byte[]> mergedQueryOutput, final StatementWithOutput[] queryOutput) throws IOException {
        if (null == queryOutput) return;
    }

    @Override
    public final void appendRows(final byte[] appendRowId, final Collection<byte[]> mergedRows, final Collection<byte[]> appendRows) {
    }
}
