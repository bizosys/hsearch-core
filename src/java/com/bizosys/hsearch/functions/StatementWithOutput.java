package com.bizosys.hsearch.functions;

import java.util.Collection;

public final class StatementWithOutput {
	public String stmtOrValue;
	public Collection<byte[]> cells;
	
	public StatementWithOutput(final String stmtOrValue, final Collection<byte[]> cells) {
		this.stmtOrValue = stmtOrValue;
		this.cells = cells;
	}
	
}
