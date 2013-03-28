package com.bizosys.hsearch.treetable.storage.sampleImpl;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryExecutor;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.storage.HSearchGenericFilter;
import com.bizosys.hsearch.treetable.storage.sampleImpl.donotmodify.HSearchTableMultiQueryProcessorImpl;

public class Filter extends HSearchGenericFilter {

	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	
	public Filter(){
	}
	public Filter(HSearchProcessingInstruction outputType, String query,Map<String, String> details) {
		super(outputType, query, details);
	}

	@Override
	public HSearchTableMultiQueryExecutor createExecutor() {
		return new HSearchTableMultiQueryExecutor(new HSearchTableMultiQueryProcessorImpl());
	}

	@Override
	public IHSearchPlugin createPlugIn(String type) throws IOException {
		if (DEBUG_ENABLED) {
			HbaseLog.l.debug(Thread.currentThread().getId()+ " > HBaseHSearchFilter : type > " + type);
		}

		if ( type.equals("ExamResult") ) {
			return new MapperExamResult();
		}


		throw new IOException("Unknown Column Type :" + type);
	}

	@Override
	public HSearchReducer getReducer() {
		return new Reducer();
	}
}
