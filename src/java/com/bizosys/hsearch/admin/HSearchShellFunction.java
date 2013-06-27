package com.bizosys.hsearch.admin;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaDefn;

public class HSearchShellFunction {

	public static StringBuilder queryKey = new StringBuilder();
	public static Map<String, String> multiQueryParts = new HashMap<String, String>();

	public static void main(String[] args) throws ParseException {
		// parseQuery1("Findings:8|*|[12:14]|!2|{2,5,6} AND Findings:9|*|[12:14]|!2|{2}");
	}

	public static void showList(String query) {
		System.out.println("initila query " + query);

		try {

			//parseQuery(query);
			URL jarUrl = new File("/tmp/571602567925882880/jar/hsearch-shell.jar").toURI().toURL();
			System.out.println("url " + jarUrl.toString());

			URLClassLoader cl = URLClassLoader.newInstance(new URL[] { jarUrl });

			HSearchProcessingInstruction outputTypeCode = new HSearchProcessingInstruction(
					HSearchProcessingInstruction.OUTPUT_COLS,
					HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS, "LIST");
			
			Class<?> Mapper = cl.loadClass("com.bizosys.hsearch.treetable.example.impl.MapperExamResult");
			Object mapper = Mapper.newInstance();
			Method setOutputCodeMethod = Mapper.getMethod("setOutputType", new Class[]{HSearchProcessingInstruction.class});
			setOutputCodeMethod.invoke(mapper, outputTypeCode);
			
			/**
			com.bizosys.hsearch.treetable.example.impl.donotmodify.HSearchTableExamResult t =
					new com.bizosys.hsearch.treetable.example.impl.donotmodify.HSearchTableExamResult();
			t.put(23, "asa", "blr", 23, 01F);
			NV nv = new NV("ExamResult_a".getBytes(), Bytes.toBytes(HBaseTableSchemaDefn.getColumnName()), t.toBytes());
			RecordScalar record = new RecordScalar("1".getBytes(),  nv );
			HWriter.getInstance(true).insertScalar("htable-test", record);
			*/
			
			Class<?> Table = cl.loadClass("com.bizosys.hsearch.treetable.example.impl.donotmodify.HSearchTableExamResult");
			Object table = Table.newInstance();

			Method getMethod = Table.getMethod("get", byte[].class,HSearchQuery.class,IHSearchPlugin.class);
			
			query = "*|*|*|*|*";
			HSearchQuery hQuery = new HSearchQuery(query);
			multiQueryParts.put("ExamResult:1", query);
			
		
			byte[] data = getAllValues("htable-test", "ExamResult_a");
			if ( null == data) System.out.println("There are no values");
			
			getMethod.invoke(table, data,hQuery,mapper);	
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	public static final byte[] getAllValues(final String tableName, final String family) throws IOException {
		
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

			scan = scan.addFamily(family.getBytes());				
			scanner = table.getScanner(scan);
			byte[] col = Bytes.toBytes(HBaseTableSchemaDefn.getColumnName());
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				byte[] storedBytes = r.getValue(family.getBytes(), col);
				if ( null == storedBytes) continue;
				System.out.println(storedBytes.length);
				return storedBytes;
			}
		} catch ( IOException ex) {
			throw ex;
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}
		return null;
	}

	public static String parseQuery(String query) throws ParseException {
		query = query.replaceAll("\\s+", " ").trim();
		int index = 1;
		int colonIndex = 0;
		String key = null;
		String[] parts = query.split(" ");
		for (String part : parts) {
			if (null == part)
				continue;
			if (part.equals("AND") || part.equals("OR") || part.equals("NOT"))
				queryKey.append(" " + part + " ");
			else {
				colonIndex = part.indexOf(':') + 1;
				key = part.substring(0, colonIndex) + index++;
				queryKey.append(key);
				multiQueryParts.put(key,part.substring(colonIndex, part.length()));
			}
		}
		for (String queryKey : multiQueryParts.keySet()) {
			System.out.println(queryKey + " = " + multiQueryParts.get(queryKey));
		}
		return null;
	}
}
