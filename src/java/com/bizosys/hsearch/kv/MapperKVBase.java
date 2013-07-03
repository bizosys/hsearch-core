/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bizosys.hsearch.kv;

import com.bizosys.hsearch.treetable.client.IHSearchPlugin;

public abstract class MapperKVBase implements IHSearchPlugin {
    
    public abstract TablePartsCallback getPart();
    
    public interface TablePartsCallback {

        public boolean onRowCols( final int key,  final Object value);
        public boolean onRowKey(final int id);
        public void onReadComplete();
    }
}
