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

package org.apache.cassandra.config.registry;

/**
 * Interface for listening to configuration property changes.
 */
public interface ConfigurationSourceListener
{
    /**
     * Called on configuration change property event occurr.
     *
     * @param name  the name of the configuration property.
     * @param eventType  the eventType of the event.
     * @param oldValue the old value of the property.
     * @param newValue the new value of the property.
     */
    void listen(String name, EventType eventType, Object oldValue, Object newValue);

    /** EventType of property change. */
    enum EventType
    {
        BEFORE_CHANGE,
        AFTER_CHANGE,
    }
}
