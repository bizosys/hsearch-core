package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.Batch;

import com.bizosys.hsearch.hbase.ColumnFamName;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.hbase.HbaseLog;

public class HSearchGenericCoProcessorFactory {
	
	public static boolean INFO_ENABLED = HbaseLog.l.isInfoEnabled();
	
	HSearchGenericFilter filter = null;
	byte[][] families = null;
	byte[][] cols = null;
	
	public HSearchGenericCoProcessorFactory(List<ColumnFamName> family_cols , HSearchGenericFilter filter) throws IOException {
		this.filter = filter;
		
		if (null == family_cols) throw new IOException("Please provide family details. Scan on all cols are not allowed");
		this.families = new byte[family_cols.size()][];
		this.cols = new byte[family_cols.size()][];
		
		int seq = -1;
		for (ColumnFamName columnFamName : family_cols) {
			seq++;
			this.families[seq] = columnFamName.family;
			this.cols[seq] = columnFamName.name;
		}

	}
	
	public final Map<byte[], long[]> execCoprocessorCounts(HTableWrapper table) throws IOException, Throwable  {

		if ( INFO_ENABLED) HbaseLog.l.info("HSearchGenericCoprocessor:execCoprocessorCounts");
		Map<byte[], long[]> output = table.table.coprocessorExec(
                HSearchGenericCoprocessor.class, null, null,
                
                new Batch.Call<HSearchGenericCoprocessor, long[]>() {
                    @Override
                    public long[] call(HSearchGenericCoprocessor counter) throws IOException {
                        return counter.getCount(families, cols, filter);
                 }
         } );
		
		return output;
		
	}
	
	public Map<byte[], double[]> execCoprocessorAggregates(HTableWrapper table) throws IOException, Throwable  {

		Map<byte[], double[]> output = table.table.coprocessorExec(
                HSearchGenericCoprocessor.class, null, null,
                
                
                new Batch.Call<HSearchGenericCoprocessor, double[]>() {
                    @Override
                    public double[] call(HSearchGenericCoprocessor counter) throws IOException {
                        return counter.getAggregates(families, cols, filter);
                 }
         } );
		
		return output;
		
	}
	
	public Map<byte[], byte[]> execCoprocessorRows(HTableWrapper table) throws IOException, Throwable  {

		Map<byte[], byte[]> output = table.table.coprocessorExec(
                HSearchGenericCoprocessor.class, null, null,
                
                
                new Batch.Call<HSearchGenericCoprocessor, byte[]>() {
                    @Override
                    public byte[] call(HSearchGenericCoprocessor counter) throws IOException {
                        return counter.getRows(families, cols, filter);
                 }
         } );
		
		return output;
	}

}
