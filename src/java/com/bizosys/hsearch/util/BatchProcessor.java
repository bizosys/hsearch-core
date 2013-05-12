/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.bizosys.hsearch.util;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.bizosys.hsearch.hbase.HbaseLog;

/**
 * @author karan
 */
public class BatchProcessor implements Runnable {
	
	private static BatchProcessor instance = null;
	
	public static BatchProcessor getInstance() {
		if ( null != instance) return instance;
		synchronized (BatchProcessor.class) {
			if ( null != instance ) return instance;
			BlockingQueue<BatchTask> msgQueue = new LinkedBlockingQueue<BatchTask>();
			instance = new BatchProcessor(msgQueue);
			Thread offlineThread = new Thread(instance);
			offlineThread.setDaemon(true);
			offlineThread.start();
		}
		return instance;
	}
	
	BlockingQueue<BatchTask> blockingQueue = null; 
	
	private BatchProcessor () {}
	
	private BatchProcessor ( BlockingQueue<BatchTask> blockingQueue){
		this.blockingQueue = blockingQueue;
	}

	public void addTask(BatchTask task) {
		if ( null == task ) return;
		if ( HbaseLog.l.isDebugEnabled() ) HbaseLog.l.debug("BatchProcessor >  A new task is lunched > " + task.getJobName());
		
		blockingQueue.add(task); 
	}
	
	public int getQueueSize() {
		if ( null == blockingQueue) return 0;
		else return blockingQueue.size();
	}
	
	/**
	 * Takes a transaction from the queue and apply this in the database.
	 */
	public void run() {
		HbaseLog.l.info("BatchProcessor > Batch processor is ready to take jobs.");
		while (true) {
			BatchTask offlineTask = null;
			try {
				offlineTask = this.blockingQueue.take(); //Request blocks here 
				if ( HbaseLog.l.isInfoEnabled() ) HbaseLog.l.info(
					"BatchProcessor > Taken from the Queue for processing - " + offlineTask.getJobName());
				boolean status = offlineTask.process();
				
			} catch (InterruptedException ex) {
				HbaseLog.l.warn(ex);
				Iterator<BatchTask> queueItr = this.blockingQueue.iterator();
				while ( queueItr.hasNext() ) HbaseLog.l.fatal("BatchProcessor > " + queueItr.next());
				break;
			} catch (Exception ex) {
				HbaseLog.l.fatal("BatchProcessor > ",  ex);
				if ( null != offlineTask) HbaseLog.l.fatal("BatchProcessor > " + offlineTask.toString());
			}
		}
	}
	
}