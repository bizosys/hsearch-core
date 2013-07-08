package com.bizosys.hsearch.treetable.example.impl;

import com.bizosys.hsearch.treetable.example.impl.donotmodify.PluginExamResultBase;
import com.bizosys.hsearch.treetable.example.impl.donotmodify.PluginExamResultBase.TablePartsCallback;

/**
 * Each thread will have 1 instance of this class.
 * For all found rows, the onRowCols/onRowKeys/... are called as we have set in the instructor.
 * @see CountClient.execute
 * When all are processed, this reports back to the main class for agrregating the result via merge.
 * @see Mapper.whole.merge
 * @author abinash
 */    
public final class MapperExamResult<T> implements TablePartsCallback {

	private PluginExamResultBase<T> context = null;
    
	public MapperExamResult(final PluginExamResultBase<T> context) {
    	this.context = context;
    }

    @Override
    public final boolean map(final int id) {
        return true;
    }

    @Override
    public final boolean map( final int age,  final String role,  final String location,  final int empid,  final float mark) {
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
    public final void close() {
    }

}
