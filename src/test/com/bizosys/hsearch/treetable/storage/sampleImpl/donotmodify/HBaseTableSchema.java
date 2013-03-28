package com.bizosys.hsearch.treetable.storage.sampleImpl.donotmodify;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.treetable.client.partition.IPartition;
import com.bizosys.hsearch.treetable.client.partition.PartitionNumeric;
import com.bizosys.hsearch.treetable.client.partition.PartitionByFirstLetter;

import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaCreator;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaDefn;

public class HBaseTableSchema {

	private static HBaseTableSchema singleton = null; 

	public static HBaseTableSchema getInstance() throws IOException {
		if ( null == singleton ) singleton = new HBaseTableSchema();
		return singleton;
	}
	
	private HBaseTableSchema() throws IOException {
		
		HBaseTableSchemaDefn.getInstance().tableName = "htable-test";
		Map<String, IPartition> columns = HBaseTableSchemaDefn.getInstance().columnPartions;
		columns.put("ExamResult",new PartitionNumeric());
		columns.get("ExamResult").setPartitionsAndRange(
			"ExamResult",
			"a,b,c,d,e,f,g,h,i,j",
			"[*:1],[1:2],[2:3],[3:4],[4:5],[5:6],[6:7],[7:8],[8:9],[9:*]",
			4);

	}

	public HBaseTableSchemaDefn getSchema() {
		return HBaseTableSchemaDefn.getInstance();
	}
	
	public void createSchema() {
		new HBaseTableSchemaCreator().init();
	}
	
	public static void main(String[] args) throws Exception {
		HBaseTableSchema.getInstance().createSchema();
	}
}
