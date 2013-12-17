package com.bizosys.hsearch.document.example.impl.donotmodify;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.treetable.client.partition.IPartition;
import com.bizosys.hsearch.treetable.client.partition.PartitionByFirstLetter;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaCreator;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaDefn;

public class HBaseTableSchema {

	private static HBaseTableSchema singleton = null; 

	public static HBaseTableSchema getInstance() throws IOException {
		if ( null == singleton ) singleton = new HBaseTableSchema();
		return singleton;
	}
	
	public String TABLE_NAME = "htable-test";
	private HBaseTableSchema() throws IOException {
		
		Map<String, IPartition> columns = HBaseTableSchemaDefn.getInstance(TABLE_NAME).columnPartions;
		columns.put("Documents",new PartitionByFirstLetter());
		columns.get("Documents").setPartitionsAndRange(
			"Documents",
			"0,1,2,3,4,5,6,7,8,9",
			"[*:1],[1:2],[2:3],[3:4],[4:5],[5:6],[6:7],[7:8],[8:9],[9:*]",
			3);


	}

	public HBaseTableSchemaDefn getSchema() {
		return HBaseTableSchemaDefn.getInstance(TABLE_NAME);
	}
	
	public void createSchema() {
		new HBaseTableSchemaCreator().init(TABLE_NAME);
	}
	
	public static void main(String[] args) throws Exception {
		HBaseTableSchema.getInstance().createSchema();
	}
}
