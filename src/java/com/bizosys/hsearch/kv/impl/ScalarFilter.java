package com.bizosys.hsearch.kv.impl;

import java.io.IOException;

import com.bizosys.hsearch.kv.MapperKV;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVBoolean;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVByte;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVDouble;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVFloat;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVInteger;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVLong;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVShort;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVString;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.treetable.storage.HSearchScalarFilter;
import com.bizosys.hsearch.util.HSearchLog;

public class ScalarFilter extends HSearchScalarFilter {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	
	public ScalarFilter(){
	}
	public ScalarFilter(HSearchProcessingInstruction outputType, String query) {
		super(outputType, query);
	}

	@Override
	public IHSearchPlugin createPlugIn() throws IOException {
		return new MapperKV();
	}

	@Override
	public IHSearchTable createTable() {
		int type = inputMapperInstructions.getOutputType();

		switch ( type) {
			case 0:
				return new HSearchTableKVBoolean();
			case 1:
				return new HSearchTableKVByte();
			case 2:
				return new HSearchTableKVShort();
			case 3:
				return new HSearchTableKVInteger();
			case 4:
				return new HSearchTableKVFloat();
			case 5:
				return new HSearchTableKVLong();				
			case 6:
				return new HSearchTableKVDouble();
			case 7:
				return new HSearchTableKVString();
			default:
				return null;
		}
		
	}
	
}
