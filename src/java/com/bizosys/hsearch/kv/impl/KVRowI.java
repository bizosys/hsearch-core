package com.bizosys.hsearch.kv.impl;

import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;

public interface KVRowI {

	public void setId(final Integer id);
	public Integer getId();
	public KVRowI create();
	public KVRowI create(final KVDataSchema dataSchema);
	public void setValue(final String name, final Object value);
	public Object getValue(final String name);
	public int getDataType(final String name);
}
