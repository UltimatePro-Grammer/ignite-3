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

/**
 * Abstract table ddl command.
 */
public class AbstractDdlCommandParams {
    /** Table name. */
    protected String tblName;

    /** Quietly ignore this command if table is not exists. */
    protected boolean ifTableExists;

    /** Schema name where this new table will be created. */
    protected String schema;

    public String tableName() {
        return tblName;
    }

    public String schemaName() {
        return schema;
    }

    /**
     * Quietly ignore if table exists flag.
     */
    public boolean ifTableExists() {
        return ifTableExists;
    }


    abstract static class AbstractBuilder<ParamT extends AbstractDdlCommandParams, BuilderT> {
        protected ParamT params;

        AbstractBuilder(ParamT params) {
            this.params = params;
        }

        public BuilderT schemaName(String schemaName) {
            params.schema = schemaName;
            return (BuilderT) this;
        }

        public BuilderT tableName(String tblName) {
            params.tblName = tblName;
            return (BuilderT) this;
        }


        /**
         * Set quietly ignore flag.
         *
         * @param ifTableNotExists Flag.
         */
        public BuilderT ifTableExists(boolean ifTableNotExists) {
            params.ifTableExists = ifTableNotExists;

            return (BuilderT) this;
        }
    }
}
