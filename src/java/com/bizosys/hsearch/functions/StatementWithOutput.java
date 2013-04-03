package com.bizosys.hsearch.functions;

import java.util.Collection;

public final class StatementWithOutput {
	
	public StatementWithOutput(String stmtOrValue, Collection<byte[]> cells) {
		this.stmtOrValue = stmtOrValue;
		this.cells = cells;
	}
	
	public String stmtOrValue;
	public Collection<byte[]> cells;
}
