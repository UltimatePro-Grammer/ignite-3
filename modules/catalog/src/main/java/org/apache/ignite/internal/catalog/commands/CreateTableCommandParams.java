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

package org.apache.ignite.internal.catalog.commands;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * CREATE TABLE statement.
 */
public class CreateTableCommandParams extends AbstractDdlCommandParams {
    public static Builder builder() {
        return new Builder();
    }

    /** Replicas number. */
    @Nullable
    private Integer replicas;

    /** Number of partitions for the new table. */
    @Nullable
    private Integer partitions;

    /** Primary key columns. */
    @Nullable
    private List<String> pkCols;

    /** Colocation columns. */
    @Nullable
    private List<String> colocationCols;

    /** Columns. */
    private List<ColumnParams> cols;

    private String dataStorage;

    @Nullable
    private Map<String, Object> dataStorageOptions;

    @Nullable
    private String zone;

    private CreateTableCommandParams() {

    }

    /**
     * Get replicas count.
     */
    @Nullable
    public Integer replicas() {
        return replicas;
    }

    /**
     * Get partitions count.
     */
    @Nullable
    public Integer partitions() {
        return partitions;
    }

    /**
     * Get table columns.
     *
     * @return Columns.
     */
    public List<ColumnParams> columns() {
        return cols;
    }

    /**
     * Get primary key columns.
     */
    public List<String> primaryKeyColumns() {
        return pkCols;
    }

    /**
     * Get colocation column names.
     *
     * @return Collocation column names.
     */
    @Nullable
    public List<String> colocationColumns() {
        return colocationCols;
    }


    /**
     * Returns data storage.
     */
    public String dataStorage() {
        return dataStorage;
    }

    /**
     * Returns data storage options.
     */
    public Map<String, Object> dataStorageOptions() {
        return dataStorageOptions == null ? Map.of() : dataStorageOptions;
    }

    /**
     * Get zone name.
     */
    @Nullable
    public String zone() {
        return zone;
    }

    public static class Builder extends AbstractBuilder<CreateTableCommandParams, Builder> {
        private Builder() {
            super(new CreateTableCommandParams());
        }

        /**
         * Set partitions count.
         */
        public Builder partitions(@Nullable Integer partitions) {
            params.partitions = partitions;

            return this;
        }


        /**
         * Set replicas count.
         */
        public Builder replicas(@Nullable Integer replicas) {
            params.replicas = replicas;

            return this;
        }

        /**
         * Set table columns.
         *
         * @param cols Columns.
         */
        public Builder columns(List<ColumnParams> cols) {
            params.cols = cols;

            return this;
        }

        /**
         * Set primary key columns.
         */
        public Builder primaryKeyColumns(List<String> pkCols) {
            params.pkCols = pkCols;

            return this;
        }

        /**
         * Sets colocation column names.
         *
         * @param colocationCols Colocation column names.
         */
        public Builder colocationColumns(@Nullable List<String> colocationCols) {
            params.colocationCols = colocationCols;

            return this;
        }

        /**
         * Sets data storage.
         *
         * @param dataStorage Data storage.
         */
        public Builder dataStorage(String dataStorage) {
            params.dataStorage = dataStorage;

            return this;
        }

        /**
         * Adds data storage option.
         *
         * @param options Options.
         */
        public Builder dataStorageOptions(Map<String, Object> options) {
            params.dataStorageOptions = options;

            return this;
        }

        /**
         * Set zone name.
         */
        public Builder zone(@Nullable String zoneName) {
            params.zone = zoneName;

            return this;
        }

        public CreateTableCommandParams build() {
            CreateTableCommandParams params0 = params;
            params = null;
            return params0;
        }
    }
}
