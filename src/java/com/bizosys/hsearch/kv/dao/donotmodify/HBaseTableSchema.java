package com.bizosys.hsearch.kv.dao.donotmodify;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.treetable.client.partition.IPartition;
import com.bizosys.hsearch.treetable.client.partition.PartitionNumeric;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaCreator;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaDefn;

public final class HBaseTableSchema {

	private static final String TABLE_NAME = "kv-store";
	private static HBaseTableSchema singleton = null; 

	public static HBaseTableSchema getInstance() throws IOException {
		if ( null == singleton ) singleton = new HBaseTableSchema();
		return singleton;
	}
	
	private HBaseTableSchema() throws IOException {
		
		Map<String, IPartition> columns = HBaseTableSchemaDefn.getInstance(TABLE_NAME).columnPartions;
		columns.put("KVInteger",new PartitionNumeric());
		columns.get("KVInteger").setPartitionsAndRange(
			"KVInteger",
			"",
			"",
			-1);
		columns.put("KVString",new PartitionNumeric());
		columns.get("KVString").setPartitionsAndRange(
			"KVString",
			"",
			"",
			-1);
		columns.put("KVLong",new PartitionNumeric());
		columns.get("KVLong").setPartitionsAndRange(
			"KVLong",
			"",
			"",
			-1);
		columns.put("KVFloat",new PartitionNumeric());
		columns.get("KVFloat").setPartitionsAndRange(
			"KVFloat",
			"",
			"",
			-1);
		columns.put("KVDouble",new PartitionNumeric());
		columns.get("KVDouble").setPartitionsAndRange(
			"KVDouble",
			"",
			"",
			-1);
		columns.put("KVByte",new PartitionNumeric());
		columns.get("KVByte").setPartitionsAndRange(
			"KVByte",
			"",
			"",
			-1);
		columns.put("KVBoolean",new PartitionNumeric());
		columns.get("KVBoolean").setPartitionsAndRange(
			"KVBoolean",
			"",
			"",
			-1);
		columns.put("KVShort",new PartitionNumeric());
		columns.get("KVShort").setPartitionsAndRange(
			"KVShort",
			"",
			"",
			-1);


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
