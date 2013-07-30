/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;

import com.bizosys.hsearch.treetable.client.IHSearchPlugin;

public abstract class PluginExamResultBase<T> implements IHSearchPlugin {
    
	public abstract TablePartsCallback createMapper(PluginExamResultBase<T> whole);

	public abstract void collect(T data) throws IOException;
	
	public final void write(T data) throws IOException  {
		this.collect(data);
		this.parts.remove();		
	}
	
	@Override
	public void setMergeId(byte[] mergeId) throws IOException {
	}
	
    public interface TablePartsCallback {

        boolean map(int id) ;
        public boolean map( final int age,  final String role,  final String location,  final int empid,  final float mark) ;
        public boolean map(int key, float value) ;
        public boolean map(float value) ;
        public void close() throws IOException ;
    }
    
    /*******************************************************************************************
     * The below sections are generic in nature and no need to be changed.
     */
    /**
     * Do not modify this section as we need to create indivisual instances per thread.
     */
    public final ThreadLocal<PluginExamResultBase.TablePartsCallback> parts = 
        	new ThreadLocal<PluginExamResultBase.TablePartsCallback>();

    public final PluginExamResultBase.TablePartsCallback getPart() {
        PluginExamResultBase.TablePartsCallback part = parts.get();
        if (null == part) {
            parts.set(this.createMapper(this));
            return parts.get();
        } 
        return part;
    }
    
}
