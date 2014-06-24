package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.treetable.example.impl.donotmodify.PluginExamResultBase.TablePartsCallback;

public final class ListMapperExamResult implements TablePartsCallback {

	private PluginExamResultBase<Map<Integer, String>> context = null;
	Map<Integer, String> rows = new HashMap<Integer, String>();

	public static class Counter {
		public int counter = 1;
	}
	
	public ListMapperExamResult(final PluginExamResultBase<Map<Integer, String>> context) {
    	this.context = context;
    }
	
    @Override
    public final boolean map(final int id) {
        return true;
    }

    @Override
    public final boolean map( final int age,  final String role,  final String location,  final int empid,  final float mark) {
    	rows.put(empid, age + "\t" + role + "\t" + location + "\t" + empid + "\t" + mark);
        return true;
    }

    @Override
    public final boolean map(final int key, final float value) {
        return true;
    }

    @Override
    public final boolean map(final float value) {
        return true;
    }

    @Override
    public final void close() throws IOException {
    	this.context.write(rows);
    	this.rows.clear();
    }

}
