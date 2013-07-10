package com.bizosys.hsearch.document.example.impl.donotmodify;

import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryProcessor;
import com.bizosys.hsearch.treetable.client.IHSearchTableCombiner;

public class HSearchTableMultiQueryProcessorImpl extends HSearchTableMultiQueryProcessor {

	public HSearchTableMultiQueryProcessorImpl() {
		super();
	}
	
	@Override
	public IHSearchTableCombiner getCombiner() {
		return new HSearchTableCombinerImpl();
	}

}
