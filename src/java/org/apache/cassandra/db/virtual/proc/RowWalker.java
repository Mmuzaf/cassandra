/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.cassandra.db.virtual.proc;

/**
 * Utility class for quick iteration over row attributes and row values.
 * Walk order is defined by {@link org.apache.cassandra.db.virtual.proc.Column} annotations and walks from a lower to higher index.
 */
public interface RowWalker<R>
{
    int count(Column.Type type);
    void visitMeta(MetadataVisitor visitor);
    void visitRow(R row, RowMetadataVisitor visitor);

    interface MetadataVisitor
    {
        <T> void accept(int index, Column.Type type, String name, Class<T> clazz);
    }

    interface RowMetadataVisitor
    {
        <T> void accept(int index, Column.Type type, String name, Class<T> clazz, T value);
    }
}
