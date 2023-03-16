/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.catalog;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.metastorage.MetaStorageManager;

/**
 * Catalog manager implementation.
 */
public class CatalogManagerImpl implements CatalogManager {
    //    private final CatalogService catalogService;
    //    private final MetaStorageManager metaStorage;

    /**
     * Constructor.
     */
    public CatalogManagerImpl(CatalogService catalogService, MetaStorageManager metaStorage) {
        //        this.catalogService = catalogService;
        //
        //        this.metaStorage = metaStorage;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<?> createTable(CreateTableParams params) {
        // Create operation future, which will returned, and saved to a map.
        CompletableFuture<Object> opFuture = CompletableFuture.completedFuture(null);

        // Creates TableDescriptor and saves it to MetaStorage.
        // Atomically:
        //        int tableId = metaStorage.get("lastTableId");
        //        metaStorage.put("table-"+tableId, new TableDescriptor(tableId, params));
        //        metaStorage.put("lastTableId", tableId+1);

        // Subscribes operation future to the MetaStorage future for failure handling
        // Operation future must be completed when got event from catalog service for expected table.

        return opFuture;
    }
}
