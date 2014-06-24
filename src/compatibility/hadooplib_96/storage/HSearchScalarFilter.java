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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchFilterProtos.HSearchScalarFilterMessage;
import com.bizosys.hsearch.util.HSearchLog;
import com.google.protobuf.ByteString;

/**
 * @author abinash
 *
 */
public abstract class HSearchScalarFilter extends FilterBase {
	
	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();
	
	/**
	 * Input Variables
	 */
	String multiQuery = null;
	String name = null;
	
	public long pluginExecutionTime = 0L;
	public long overallExecutionTime = 0L;
	
	protected HSearchProcessingInstruction inputMapperInstructions = new HSearchProcessingInstruction();
	byte[] inputRowsToIncludeB = null;
	List<byte[]>  inputRowsList = null;
	SortedBytesArray rowsToInclude = null;
	byte[] matchingIds = null;
	
	/**
	 * Output Variables
	 */
	
	HSearchQuery query = null; 
	IHSearchTable table = null;
	IHSearchPlugin plugin = null;
	boolean skipFiltering = true;
	Collection<byte[]> dataCarrier = new ArrayList<byte[]>();


	public HSearchScalarFilter(){}

	public HSearchScalarFilter(final HSearchProcessingInstruction outputType,final String query) {
		this.multiQuery = query;
		this.inputMapperInstructions = outputType;
	}
	
	public void setMatchingRows(List<byte[]> inputRowsList) {
		this.inputRowsList = inputRowsList;
	}
	
	public void setMatchingIds(byte[] matchingIds) {
		this.matchingIds = matchingIds;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		if ( null == name) {
			name = this.getClass().getName();
		}
		return this.name;
	}

	/**
	 * @return The filter serialized using pb
	 * @throws IOException 
	 */
	@Override
	public byte [] toByteArray() throws IOException {

		HSearchScalarFilterMessage filterMessage = getScalarFilterMessage(this);

		if ( DEBUG_ENABLED ) {
			HSearchLog.l.debug("Sending to HBase : " + filterMessage.toString());
		}
		
		return filterMessage.toByteArray();		    
	}
	
	public static HSearchScalarFilterMessage getScalarFilterMessage(HSearchScalarFilter instance)
			throws IOException {

		HSearchScalarFilterMessage.Builder builder = HSearchScalarFilterMessage.newBuilder();

		builder.setFilterClassName(instance.getClass().getName());
		builder.setInputMapperInstructions(instance.inputMapperInstructions.toString());
		builder.setMultiQuery(instance.multiQuery);
		
		if(null != instance.matchingIds)
			builder.setMatchingIds(ByteString.copyFrom(instance.matchingIds));
		else
			builder.setMatchingIds(ByteString.copyFrom(new byte[0]));

		if ( null != instance.inputRowsList) {
			if ( instance.inputRowsList.size() > 0 ) {
				instance.inputRowsToIncludeB = SortedBytesArray.getInstanceArr().toBytes(instance.inputRowsList);
				builder.setInputRowsToIncludeB(ByteString.copyFrom(instance.inputRowsToIncludeB));
			}
		}
		return builder.build();
	}

	/**
	 * @param pbBytes A pb serialized {@link PrefixFilter} instance
	 * @return An instance of {@link PrefixFilter} made from <code>bytes</code>
	 * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
	 * 
	 */
	public static HSearchScalarFilter parseFrom(final byte [] pbBytes)throws DeserializationException {
		
		HSearchScalarFilter scalarFilter = null;
		
		try {

		    int length = null == pbBytes ? 0 : pbBytes.length;
			if(0 == length)
				throw new IOException("Invalid Query");

			
			if ( DEBUG_ENABLED) {
				HSearchLog.l.debug("Total bytes Received @ HSearchScalarFilter:" + length);
			}

			HSearchScalarFilterMessage filterMessage = HSearchScalarFilterMessage.parseFrom(pbBytes);
			scalarFilter = getScalarFilter(filterMessage);
			
		} catch (Exception ex) {
			HSearchLog.l.fatal(ex);
			ex.printStackTrace();
			throw new DeserializationException(ex);
		}

		return scalarFilter;
	}
	
	public static HSearchScalarFilter getScalarFilter(HSearchScalarFilterMessage filterMessage)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, IOException, ParseException {
		
		String className = filterMessage.getFilterClassName();
		//TODO:Create factory for Generic filter instead of instantiating using reflection
		HSearchScalarFilter scalarFilter = (HSearchScalarFilter) Class.forName(className).newInstance();
		

		if(filterMessage.hasInputRowsToIncludeB()){
			scalarFilter.rowsToInclude = SortedBytesArray.getInstanceArr();
			scalarFilter.rowsToInclude.parse(filterMessage.getInputRowsToIncludeB().toByteArray());
		}
		
		scalarFilter.multiQuery = filterMessage.getMultiQuery();
		if ( DEBUG_ENABLED ) {
			HSearchLog.l.debug("HBase Region Server: Multi Query" +  scalarFilter.multiQuery);
		}
		
		String instruction = filterMessage.getInputMapperInstructions();
		if (instruction.length() == 0  ) throw new IOException("Unknown result output type.");
		
		scalarFilter.inputMapperInstructions = new HSearchProcessingInstruction(instruction);

		//matching ids
		scalarFilter.matchingIds = filterMessage.getMatchingIds().toByteArray();
		
		scalarFilter.query = new HSearchQuery(scalarFilter.multiQuery);
		scalarFilter.table = scalarFilter.createTable();
		if ( null != scalarFilter.table) {
			scalarFilter.plugin = scalarFilter.createPlugIn();
			if ( null != scalarFilter.plugin) {
				scalarFilter.plugin.setOutputType(scalarFilter.inputMapperInstructions);
				if(0 != scalarFilter.matchingIds.length)
					scalarFilter.plugin.setMergeId(scalarFilter.matchingIds);
				scalarFilter.skipFiltering = false;
			}
		}
		
		return scalarFilter;
	}

	@Override
	public void filterRowCells(List<Cell> cellL) throws IOException {
		if ( skipFiltering ) return;
		
		if ( null == cellL) return;
		int cellT = cellL.size();
		if ( 0 == cellT) return;
		
		if ( DEBUG_ENABLED ) {
			HSearchLog.l.debug("Processing @ Region Server : filterRow" );
		}
		
		try {
			
			List<Cell> cellLFiltered = new ArrayList<Cell>();
			
			for (Cell cell : cellL) {
				if ( null == cell) continue;

				byte[] inputData = CellUtil.cloneValue(cell);
				if ( null == inputData) continue;
				
				switch ( this.inputMapperInstructions.getCallbackType()) {
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS:
						table.get(inputData, this.query, plugin);
						break;
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_ID:
						table.keySet(inputData, this.query, plugin);
						break;
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_VAL:
						table.values(inputData, this.query, plugin);
						break;
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_IDVAL:
						table.keyValues(inputData, this.query, plugin);
						break;
					default:
						throw new IOException("Unknown output type:" + this.inputMapperInstructions.getCallbackType());
				}
				Cell newCell = CellUtil.createCell(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), cell.getTimestamp(), cell.getTypeByte(), SortedBytesArray.getInstance().toBytes(dataCarrier));
				plugin.getResultSingleQuery(dataCarrier);
				cellLFiltered.add(newCell);
				dataCarrier.clear();
			}
			cellL.clear();
			cellL.addAll(cellLFiltered);
			
			if ( DEBUG_ENABLED ) {
				HSearchLog.l.debug("queryData HSearchTableParts creation. ");
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			HSearchLog.l.fatal(ex);
		}
	}
		
	@Override
	public final boolean hasFilterRow() {
		return true;
	}	
		
	@Override
	public final boolean filterRowKey(final byte[] rowKey, final int offset, final int length) {
		
		if ( null == rowsToInclude) return false;
		byte[] exactRowBytes = new byte[length];
		try {
			System.arraycopy(rowKey, offset, exactRowBytes, 0, length);
			if ( rowsToInclude.getEqualToIndex(exactRowBytes) >= 0 ) {
				//System.out.println("Allow row:" + new String(exactRowBytes));
				return false;
			} else {
				//System.out.println("Disallow row:" + new String(exactRowBytes));
				return true;
			}
			
		} catch (IOException ex) {
			int scopeToTheseRowsT = ( null == rowsToInclude) ? 0 : rowsToInclude.getSize();
			String rowKeyStr = ( null == rowKey) ? "Null row key" : new String(rowKey);
			String errMsg = "Error while finding fileration criteria for the row , " + rowKeyStr 
				+ "\n" + ex.getMessage() + "\n" + 
				"With search scope inside id count : " + scopeToTheseRowsT;
			System.err.println(errMsg);
			HSearchLog.l.fatal(errMsg, ex);
			
			return false;
		}
	}	

	public final void deserialize(final byte[] input, final Collection<byte[]> output) throws IOException {
		SortedBytesArray.getInstance().parse(input).values(output);
	}
	
	public abstract IHSearchPlugin createPlugIn() throws IOException ;
	public abstract IHSearchTable createTable();
	
	
	/**
	 * Override this method if you want to set more filters in processing.
	 * 
	 	FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		RowFilter filter1 = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row-22")) );
		list.addFilter(filter1);
		list.addFilter(this);
		return list;
		
	 * @return
	 */
	public final FilterList getFilters() {
		return null;
	}
	
	/**
	 * Any information to be configured before starting the filtration process.
	 */
	public final void configure() {
	}
	
	/**
	 * At the end release the resources.
	 */
	public final void close() {
	}
}
	