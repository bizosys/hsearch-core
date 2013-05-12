/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.treetable.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import com.bizosys.hsearch.hbase.HbaseLog;

/**
 * @author abinash
 *
 */
public abstract class HSearchBytesFilter implements Filter {
	
	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HbaseLog.l.isInfoEnabled();
	
	protected byte[] state = null;	
	
	public HSearchBytesFilter(){
	}
	
	public HSearchBytesFilter(final byte[] state){
		this.state = state;
	}
	
	@Override
	public final void write(final DataOutput out) throws IOException {
		out.writeInt(state.length);
		out.write(state);
	}	

	@Override
	public final void readFields(final DataInput in) throws IOException {
		try {
			int length = in.readInt();
			if ( 0 == length) throw new IOException("Invalid Input");
			
			state = new byte[length];
			in.readFully(state, 0, length);

		} catch (Exception ex) {
			HbaseLog.l.fatal("Error at deserialization of filter:" + ex.getMessage() , ex);
			throw new IOException(ex);
		} 
	}
	
	@Override
	public final void filterRow(final List<KeyValue> kvL) {
		if ( null == kvL) return;
		int kvT = kvL.size();
		if ( 0 == kvT) return;
		
		try {
			for (KeyValue kv : kvL) {
				if ( null == kv) continue;

				byte[] inputData = kv.getValue();
				if ( null == inputData) continue;
				
				processColumn(kv);
			}
			
			processRow(kvL);
			
		} catch (Exception ex) {
			HbaseLog.l.fatal(ex);
			ex.printStackTrace(System.err);
		} 
	}

	public abstract void processColumn(KeyValue cell) throws IOException;
	public abstract void processRow(List<KeyValue> row) throws IOException;
	public abstract byte[] processRows() throws IOException;
	

	@Override
	public void reset() {
	}	
	
	@Override
	public boolean hasFilterRow() {
		return true;
	}	
	
	@Override
	public KeyValue getNextKeyHint(final KeyValue arg0) {
		return null;
	}	
	
	@Override
	public boolean filterRowKey(final byte[] rowKey, final int offset, final int length) {
		return false;
	}
	
	@Override
	public boolean filterAllRemaining() {
		return false;
	}
	
	@Override
	public boolean filterRow() {
		return false;
	}
	
	@Override
	public ReturnCode filterKeyValue(final KeyValue arg0) {
		return ReturnCode.INCLUDE;
	}	
	
	/**
	 * Version 0.94 FIX
	 */
	@Override
	public KeyValue transform(final KeyValue arg0) {
		return arg0;
	}
	
	public FilterList getFilters() {
		return null;
	}
}