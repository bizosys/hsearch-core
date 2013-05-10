/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import com.bizosys.hsearch.treetable.client.IHSearchPlugin;

public abstract class PluginExamResultBase implements IHSearchPlugin {
    
    public abstract TablePartsCallback getPart();
    
    public interface TablePartsCallback {

        boolean onRowKey(int id);
        public boolean onRowCols( final int age,  final String role,  final String location,  final int empid,  final float mark);
        public boolean onRowKeyValue(int key, float value);
        public boolean onRowValue(float value);
        public void onReadComplete();
    }
}
