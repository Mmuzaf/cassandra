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

package org.apache.cassandra.sysview;


import javax.annotation.Nullable;

import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Utility class for quick iteration over row properties.
 */
public interface SystemViewWalker<R extends ViewRow> {
    /** @return Count of a row properties. */
    int count();

    /**
     * Calls visitor for each row attribute.
     *
     * @param visitor Attribute visitor.
     */
    void visitAll(AttributeVisitor visitor);

    /**
     * Calls visitor for each row attribute.
     * Value of the attribute also provided.
     *
     * @param row Row to iterate.
     * @param visitor Attribute visitor.
     */
    void visitAll(R row, AttributeWithValueVisitor visitor);

    /** Attribute visitor. */
    interface AttributeVisitor {
        /**
         * Visit some object property.
         * @param type Type.
         * @param name Name.
         * @param clazz Value class.
         * @param <T> Value type.
         */
        <T> void accept(ColumnMetadata.Kind type, String name, Class<T> clazz);
    }

    /** Attribute visitor. */
    interface AttributeWithValueVisitor {
        /**
         * Visit attribute. Attribute value is object.
         *
         * @param type Type.
         * @param name Name.
         * @param clazz Class.
         * @param value Value.
         * @param <T> Value type.
         */
        <T> void accept(ColumnMetadata.Kind type, String name, Class<T> clazz, @Nullable T value);

        /**
         * Visit attribute. Attribute value is {@code boolean} primitive.
         *
         * @param type Type.
         * @param name Name.
         * @param value Value.
         */
        void acceptBoolean(ColumnMetadata.Kind type, String name, boolean value);

        /**
         * Visit attribute. Attribute value is {@code char} primitive.
         *
         * @param type Type.
         * @param name Name.
         * @param value Value.
         */
        void acceptChar(ColumnMetadata.Kind type, String name, char value);

        /**
         * Visit attribute. Attribute value is {@code byte} primitive.
         *
         * @param type Type.
         * @param name Name.
         * @param value Value.
         */
        void acceptByte(ColumnMetadata.Kind type, String name, byte value);

        /**
         * Visit attribute. Attribute value is {@code short} primitive.
         *
         * @param type Type.
         * @param name Name.
         * @param value Value.
         */
        void acceptShort(ColumnMetadata.Kind type, String name, short value);

        /**
         * Visit attribute. Attribute value is {@code int} primitive.
         *
         * @param type Type.
         * @param name Name.
         * @param value Value.
         */
        void acceptInt(ColumnMetadata.Kind type, String name, int value);

        /**
         * Visit attribute. Attribute value is {@code long} primitive.
         *
         * @param type Type.
         * @param name Name.
         * @param value Value.
         */
        void acceptLong(ColumnMetadata.Kind type, String name, long value);

        /**
         * Visit attribute. Attribute value is {@code float} primitive.
         *
         * @param type Type.
         * @param name Name.
         * @param value Value.
         */
        void acceptFloat(ColumnMetadata.Kind type, String name, float value);

        /**
         * Visit attribute. Attribute value is {@code double} primitive.
         *
         * @param type Type.
         * @param name Name.
         * @param value Value.
         */
        void acceptDouble(ColumnMetadata.Kind type, String name, double value);
    }
}

