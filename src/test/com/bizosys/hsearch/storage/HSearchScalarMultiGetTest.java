package com.bizosys.hsearch.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.ByteArrays.ArrayBytes;
import com.bizosys.hsearch.hbase.ColumnFamName;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellKeyValue;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.treetable.storage.HSearchScalarFilter;
import com.bizosys.hsearch.treetable.storage.HSearchMultiGetCoProcessorProxy;
import com.bizosys.hsearch.util.HSearchLog;
import com.oneline.ferrari.TestAll;

public class HSearchScalarMultiGetTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[1];  
		
		public static void main(String[] args) throws Throwable {
			HSearchScalarMultiGetTest t = new HSearchScalarMultiGetTest();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.test2Rows();
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		

		/**
		 * -1_age
-1_comments
-1_empid
-1_location
-1_marks
-1_role
-1_sex
A_-_1
A_-_4
A_-_5
A_-_6
A_-_7
A_-_8
A_1_0
A_1_2
A_1_3
A_1_4
A_1_5
A_1_6
A_4_5
A_age
A_comments
A_empid
A_location
A_marks
A_role
A_sex
B_-_1
B_-_4
B_-_5
B_-_6
B_-_7
B_-_8
B_1_0
B_1_2
B_1_3
B_1_4
B_1_5
B_1_6
B_4_5
B_age
B_comments
B_empid
B_location
B_marks
B_role
B_sex
		 * @throws Throwable 
		 */
		public void test2Rows() throws Throwable {	
			HBaseFacade facade = null;
			HTableWrapper table = null;
			try {
				facade = HBaseFacade.getInstance();
				table = facade.getTable("examresult");
				byte[][] rows = new byte[][] {
					"B_role".getBytes(), "B_sex".getBytes() 
				};
				
				Map<Long, byte[]> uniqueRegionsWithStartRow = new HashMap<Long, byte[]>();
				
				for (byte[] row : rows) {
					HRegionInfo regionInfo = table.getRegionLocation(row).getRegionInfo();
					long regionName = regionInfo.getRegionId();
					if ( uniqueRegionsWithStartRow.containsKey(regionName)) continue;
					System.out.println("Adding Region:" + regionName);
					uniqueRegionsWithStartRow.put(regionName, row);
				}
				
				System.out.println("Total Regions to hit :" + uniqueRegionsWithStartRow.size());
				
				for (byte[] row : uniqueRegionsWithStartRow.values()) {
					ColumnFamName cf = new ColumnFamName("1".getBytes(), new byte[]{0});
					HSearchMultiGetCoProcessorProxy p = new HSearchMultiGetCoProcessorProxy(cf,null, rows);
					Map<String, byte[]> res = new HashMap<String, byte[]>();
					p.execCoprocessorRows(res, table,row);
					System.out.println ( res.keySet().toString());
				}

			} catch ( IOException ex) {
				throw ex;
			} finally {
				if ( null != table ) facade.putTable(table);
			}
		}	
				
}
