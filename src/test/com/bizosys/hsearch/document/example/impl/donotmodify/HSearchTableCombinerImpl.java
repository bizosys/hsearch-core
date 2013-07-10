package com.bizosys.hsearch.document.example.impl.donotmodify;

import java.io.IOException;

import com.bizosys.hsearch.treetable.client.HSearchTableCombiner;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

public class HSearchTableCombinerImpl extends HSearchTableCombiner {

	@Override
	public IHSearchTable buildTable(String tableType) throws IOException {
		
		if ( tableType.equals("Documents")) return new HSearchTableDocuments();

		
		throw new IOException("Class not found. Missing class HSearchTable" + tableType );
		
	}
}
