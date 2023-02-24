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

#include "ignite/client/table/table.h"
#include "ignite/client/detail/table/table_impl.h"

namespace ignite {

const std::string &table::name() const noexcept {
    return m_impl->name();
}

record_view<ignite_tuple> table::record_binary_view() const noexcept {
    return record_view<ignite_tuple>{m_impl};
}

key_value_view<ignite_tuple, ignite_tuple> table::key_value_binary_view() const noexcept {
    return key_value_view<ignite_tuple, ignite_tuple>{m_impl};
}

} // namespace ignite
