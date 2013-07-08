package com.bizosys.hsearch.treetable.example.impl;

import java.io.IOException;
import java.util.Collection;

import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.functions.StatementWithOutput;

public class Reducer implements HSearchReducer {

    @Override
    public final void appendCols(final StatementWithOutput[] queryOutput, final Collection<byte[]> mergedQueryOutput) throws IOException {
    }

    @Override
    public final void appendRows(final Collection<byte[]> mergedB, final Collection<byte[]> appendB) {
    }

    @Override
    public final void appendRows(final Collection<byte[]> mergedRows, final byte[] appendRowId, final Collection<byte[]> appendRows) {
        appendRows(mergedRows, appendRows);

    }
}
