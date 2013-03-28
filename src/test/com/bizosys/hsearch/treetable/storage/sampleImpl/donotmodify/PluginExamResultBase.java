/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bizosys.hsearch.treetable.storage.sampleImpl.donotmodify;

import com.bizosys.hsearch.treetable.client.IHSearchPlugin;

public abstract class PluginExamResultBase implements IHSearchPlugin {
    
    public abstract TablePartsCallback getPart();
    
    public interface TablePartsCallback {

        boolean onRowKey(int id);
        public boolean onRowCols(int cell1, String cell2, String cell3, int cell4, float cell5);
        public boolean onRowKeyValue(int key, float value);
        public boolean onRowValue(float value);
        public void onReadComplete();
    }
}
