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

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchFilterProtos.HSearchBytesFilterMessage;
import com.bizosys.hsearch.util.HSearchLog;
import com.google.protobuf.ByteString;

/**
 * @author abinash
 *
 */
public abstract class HSearchBytesFilter extends FilterBase {
	
	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();
	
	protected byte[] state = null;	
		
	public HSearchBytesFilter(final byte[] state){
		this.state = state;
	}

	/**
	 * @return The filter serialized using pb
	 * @throws IOException 
	 */
	@Override
	public byte [] toByteArray() throws IOException {

		HSearchBytesFilterMessage filterMessage = getBytesFilterMessage(this);
		if ( DEBUG_ENABLED ) {
			HSearchLog.l.debug("Sending to HBase : " + filterMessage.toString());
		}
		
		return filterMessage.toByteArray();		    
	}
	
	public static HSearchBytesFilterMessage getBytesFilterMessage(HSearchBytesFilter instance)
			throws IOException {
		
		HSearchBytesFilterMessage.Builder builder = HSearchBytesFilterMessage.newBuilder();

		builder.setFilterClassName(instance.getClass().getName());
		builder.setState(ByteString.copyFrom(instance.state));
		return builder.build();
	}
	
	/**
	 * @param pbBytes A pb serialized {@link PrefixFilter} instance
	 * @return An instance of {@link PrefixFilter} made from <code>bytes</code>
	 * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
	 * 
	 */
	public static HSearchBytesFilter parseFrom(final byte [] pbBytes)throws DeserializationException {
		
		HSearchBytesFilter bytesFilter = null;
		
		try {

		    int length = null == pbBytes ? 0 : pbBytes.length;
			if(0 == length)
				throw new IOException("Invalid Query");

			if ( DEBUG_ENABLED) {
				HSearchLog.l.debug("Total bytes Received @ HSearchBytesFilter:" + length);
			}
			
			HSearchBytesFilterMessage filterMessage = HSearchBytesFilterMessage.parseFrom(pbBytes);
			bytesFilter = getBytesFilter(filterMessage);

		} catch (Exception ex) {
			HSearchLog.l.fatal(ex);
			ex.printStackTrace();
			throw new DeserializationException(ex);
		}

		return bytesFilter;
	}
	
	public static HSearchBytesFilter getBytesFilter(HSearchBytesFilterMessage filterMessage)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, IOException, ParseException {

		String className = filterMessage.getFilterClassName();
		//TODO:Create factory for Generic filter instead of instantiating using reflection
		HSearchBytesFilter bytesFilter = (HSearchBytesFilter) Class.forName(className).newInstance();
		
		bytesFilter.state = filterMessage.getState().toByteArray();
		return bytesFilter;
	}

	@Override
	public final void filterRowCells(final List<Cell> cellL) {
		if ( null == cellL) return;
		int cellT = cellL.size();
		if ( 0 == cellT) return;
		
		try {
			for (Cell cell : cellL) {
				if ( null == cell) continue;

				byte[] inputData = CellUtil.cloneValue(cell);
				if ( null == inputData) continue;
				
				processColumn(cell);
			}
			
			processRow(cellL);
			
		} catch (Exception ex) {
			HSearchLog.l.fatal(ex);
			ex.printStackTrace(System.err);
		} 
	}

	public abstract void processColumn(Cell cell) throws IOException;
	public abstract void processRow(List<Cell> row) throws IOException;
	public abstract byte[] processRows() throws IOException;
	
	
	@Override
	public boolean hasFilterRow() {
		return true;
	}	
	
	public FilterList getFilters() {
		return null;
	}
}