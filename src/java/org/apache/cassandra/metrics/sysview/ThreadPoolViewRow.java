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

package org.apache.cassandra.metrics.sysview;

import java.util.Map;

import org.apache.cassandra.config.sysview.ConfigViewRow;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.sysview.SystemViewWalker;
import org.apache.cassandra.sysview.ViewRow;

public class ThreadPoolViewRow implements ViewRow {
  private final Map.Entry<String, ThreadPoolMetrics> entry;
  public ThreadPoolViewRow(Map.Entry<String, ThreadPoolMetrics> entry)
    {this.entry = entry;}

  // Number of thread poll columns is simplified for visibility.
  public String getPoolName()
    {return entry.getKey();}
  public Integer getActiveTasks()
    {return entry.getValue().activeTasks.getValue();}
  public Integer getPendingTasks()
    {return entry.getValue().pendingTasks.getValue();}

  // The SystemViewWalker's ColumnMetadata.Kind is simplified for visibility.
  public SystemViewWalker<ThreadPoolViewRow> walk() {
    return new SystemViewWalker<ThreadPoolViewRow>() {
      public int count() {return 3;}
      public void visitAll(AttributeVisitor v) {
//        v.accept(PARTITION_KEY, "pool_name", String.class);
//        v.accept(REGULAR, "active_tasks", Integer.class);
//        v.accept(ColumnMetadata.Kind.REGULAR, "pending_tasks", Integer.class);
      }
      public void visitAll(ThreadPoolViewRow row, AttributeWithValueVisitor v) {
//        v.accept(PARTITION_KEY, "pool_name", String.class, row.getPoolName());
//        v.accept(REGULAR, "active_tasks", Integer.class, row.getActiveTasks());
//        v.accept(REGULAR, "pending_tasks", Integer.class,row.getPendingTasks());
      }
    };
  }
}
