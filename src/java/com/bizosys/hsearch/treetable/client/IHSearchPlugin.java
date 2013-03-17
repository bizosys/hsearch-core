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
package com.bizosys.hsearch.treetable.client;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.bizosys.hsearch.federate.FederatedFacade;

public interface IHSearchPlugin {
	void setOutputType(HSearchProcessingInstruction outputTypeCode);
	void cleanupValuesFromLastRun() ;
	void onFilterationComplete();
	
	Collection<String> getUniqueMatchingDocumentIds() throws IOException;
	void getResultMultiQuery( List<FederatedFacade<String, String>.IRowId> matchedIds, Collection<byte[]> rows) throws IOException;
	void getResultSingleQuery( Collection<byte[]> rows) throws IOException;
}
