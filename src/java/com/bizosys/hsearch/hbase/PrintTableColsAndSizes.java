package com.bizosys.hsearch.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;

/**
 * Print ProductId, Column Name and The Value Size
 * @author abinash
 *
 */
public class PrintTableColsAndSizes {

	public static final String DATE_FORMAT = "yyyy:MM:dd:HH:mm:ss";
	public static SimpleDateFormat dateFormatter = new SimpleDateFormat(DATE_FORMAT);
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws IOException, ParseException {
		
		if ( args.length < 2) {
			System.err.println("Usage " + PrintTableColsAndSizes.class.getName() + "  <<TABLE>>  <<FAMILY>> [JUMP_STEP] [FROM_TIME yyyy:MM:dd:HH:mm:ss] [TO_TIME yyyy:MM:dd:HH:mm:ss]");
			return;
		}
		
		String tableName = args[0]; 
		String familyName = args[1];
		int jumpStep = 1;
		Date startTime = null;
		Date endTime = new Date();
		
		if (args.length > 2) {
			jumpStep = Integer.parseInt(args[2]);
		}
		
		if (args.length > 3) {
			startTime = dateFormatter.parse(args[3]);
		}

		if (args.length > 4) {
			endTime = dateFormatter.parse(args[4]);
		}

		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;

		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addFamily(familyName.getBytes());
			if ( startTime != null) {
				scan = scan.setTimeRange(startTime.getTime(), endTime.getTime());
			}
			
			scanner = table.getScanner(scan);
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				for (KeyValue kv : r.list()) {
					System.out.println(new String(r.getRow()) + "\t" + new String(kv.getQualifier()) + "\t" + kv.getValueLength());
				}
			}
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}		

	}

}
