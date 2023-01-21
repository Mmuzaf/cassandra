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

package org.apache.cassandra.utils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.commitlog.CommitLogMBean;

/**
 * Helper class.
 */
public class Gen
{

    public static void main(String[] args)
    {
        List<Class> intfc = new ArrayList<Class>();
        intfc.add(CommitLogMBean.class);
        for (Class c : intfc)
            System.out.println(gen(c));
    }

    private static String gen(Class c)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("return new MBeanInfo(\"");
        sb.append(c.getName());
        sb.append("\",");
        sb.append("\n\"Desc\",\r\n" +
                  "               null,null, \r\n" +
                  "               new MBeanOperationInfo[]{");
        boolean first = true;
        for (Method m : c.getDeclaredMethods())
        {
            if (!first)
                sb.append(",");
            first = false;
            sb.append("\nnew MBeanOperationInfo(");
            sb.append("\"" + m.getName() + "\",\"" + m.getName() + "\", new MBeanParameterInfo[]{");
            boolean firstparam = true;
            for (Class param : m.getParameterTypes())
            {
                if (!firstparam)
                    sb.append(",");
                firstparam = false;
                sb.append("new MBeanParameterInfo(\"ParamName\",\"");
                sb.append(param.getName()).append("\",");
                sb.append("\"Description\")");
            }
            sb.append("},\"" + m.getReturnType().getName() + "\",0)");
            sb.append("\n");
        }

        sb.append("}\n,null);");
        return sb.toString();
    }
}