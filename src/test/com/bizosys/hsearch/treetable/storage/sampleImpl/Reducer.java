package com.bizosys.hsearch.treetable.storage.sampleImpl;

import java.util.Collection;

import com.bizosys.hsearch.functions.HSearchReducer;

public class Reducer implements HSearchReducer {

    @Override
    public void appendCols(Collection<byte[]> mergedB, Collection<byte[]> appendB) {

        if (null == appendB) return;

        System.out.println("appendCols :" + mergedB.size() + ":" + appendB.size() );
        if (appendB.size() == 0) return;

        for (byte[] bs : appendB) {
			if ( null == bs) continue;
			mergedB.add(bs);
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
