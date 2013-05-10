package com.bizosys.hsearch.treetable.example.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.functions.StatementWithOutput;

public class Reducer implements HSearchReducer {

    @Override
    public final void appendCols(final StatementWithOutput[] queryOutput, final Collection<byte[]> mergedQueryOutput) throws IOException {

        if (null == queryOutput) return;

        /**
         * TODO:// De-serialize and compute.
         */
    }

    @Override
    public final void appendRows(final Collection<byte[]> mergedB, final Collection<byte[]> appendB) {

        if (null == appendB) return;
        if (appendB.size() == 0) return;

        byte[] append = appendB.iterator().next();
        if (null == append) return;
        
        byte[] merged = null;
        if (mergedB.size() == 0) {
            merged = new byte[append.length];
            Arrays.fill(merged, (byte) 0);
        } else {
            merged = mergedB.iterator().next();
        }

        /**
         * De-serialize and compute.
         */

        /**
         * Serialize at the end
         */

        mergedB.clear();

        /**
         * Add the processed bytes to merged container
         */
    }

    @Override
    public final void appendRows(final Collection<byte[]> mergedRows, final byte[] appendRowId, final Collection<byte[]> appendRows) {
        appendRows(mergedRows, appendRows);

    }
}
