package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.treetable.example.impl.donotmodify.PluginExamResultBase.TablePartsCallback;

public final class CountMapperExamResult implements TablePartsCallback {

	private PluginExamResultBase<Map<Integer, Counter>> context = null;

	public static class Counter {
		public int counter = 1;
	}
	
	Map<Integer, Counter> idWithCounter = new HashMap<Integer, CountMapperExamResult.Counter>();	
    
	public CountMapperExamResult(final PluginExamResultBase<Map<Integer, Counter>> context) {
    	this.context = context;
    }
	
    @Override
    public final boolean map(final int id) {
    	System.out.println("Id:" + id);
    	if ( idWithCounter.containsKey(id)) idWithCounter.get(id).counter++;
    	else idWithCounter.put(id, new Counter());
        return true;
    }

    @Override
    public final boolean map( final int age,  final String role,  final String location,  final int empid,  final float mark) {
    	System.out.println("cols:" + empid);
        return true;
    }

    @Override
    public final boolean map(final int key, final float value) {
    	System.out.println("kv:" + key);
        return true;
    }

    @Override
    public final boolean map(final float value) {
    	System.out.println("val:" + value);
        return true;
    }

    @Override
    public final void close() throws IOException {
    	this.context.write(idWithCounter);
    	this.idWithCounter.clear();
    }

}
