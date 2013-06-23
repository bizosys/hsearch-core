package com.bizosys.hsearch.kv.impl;

import java.util.List;

public interface IEnricher {
	public void enrich(List<KVRowI> rows);
}
