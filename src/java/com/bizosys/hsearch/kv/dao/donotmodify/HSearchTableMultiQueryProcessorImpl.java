package com.bizosys.hsearch.kv.dao.donotmodify;

import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableCombinerImpl;
import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryProcessor;
import com.bizosys.hsearch.treetable.client.IHSearchTableCombiner;

public final class HSearchTableMultiQueryProcessorImpl extends HSearchTableMultiQueryProcessor {

	public HSearchTableMultiQueryProcessorImpl() {
		super();
	}
	
	@Override
	public final IHSearchTableCombiner getCombiner() {
		return new HSearchTableCombinerImpl();
	}

}
