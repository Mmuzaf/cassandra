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

package org.apache.cassandra.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenMBeanAttributeInfo;
import javax.management.openmbean.OpenMBeanAttributeInfoSupport;
import javax.management.openmbean.OpenMBeanInfoSupport;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.sysview.SystemView;
import org.apache.cassandra.sysview.SystemViewWalker;
import org.apache.cassandra.sysview.ViewRow;

/**
 * JMX bean to expose specific {@link org.apache.cassandra.sysview.SystemView} data.
 * TODO MX4J is better to use AbstractDynamicMBean here, right (?)
 */
public class SystemViewMBean<R extends ViewRow> implements DynamicMBean
{
    public static final String VIEWS = "views";
    public static final String ID = "systemViewRowId";
    private static final Map<Class<?>, SimpleType<?>> CLASS_TO_SIMPLE_TYPE_MAP = new HashMap<>();

    static {
        CLASS_TO_SIMPLE_TYPE_MAP.put(String.class, SimpleType.STRING);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Class.class, SimpleType.STRING);
        CLASS_TO_SIMPLE_TYPE_MAP.put(Enum.class, SimpleType.STRING);
        CLASS_TO_SIMPLE_TYPE_MAP.put(boolean.class, SimpleType.BOOLEAN);
        CLASS_TO_SIMPLE_TYPE_MAP.put(byte.class, SimpleType.BYTE);
        CLASS_TO_SIMPLE_TYPE_MAP.put(short.class, SimpleType.SHORT);
        CLASS_TO_SIMPLE_TYPE_MAP.put(int.class, SimpleType.INTEGER);
        CLASS_TO_SIMPLE_TYPE_MAP.put(long.class, SimpleType.LONG);
        CLASS_TO_SIMPLE_TYPE_MAP.put(char.class, SimpleType.CHARACTER);
        CLASS_TO_SIMPLE_TYPE_MAP.put(float.class, SimpleType.FLOAT);
        CLASS_TO_SIMPLE_TYPE_MAP.put(double.class, SimpleType.DOUBLE);
    }

    private final SystemView<R> systemView;
    private final MBeanInfo info;
    private final CompositeType rowType;
    private final TabularType sysViewType;

    public SystemViewMBean(SystemView<R> systemView)
    {
        this.systemView = systemView;

        int cnt = systemView.walker().count();
        String[] fields = new String[cnt + 1];
        OpenType[] types = new OpenType[cnt + 1];
        int index = 0;

        systemView.walker().visitAll(new SystemViewWalker.AttributeVisitor()
        {
            @Override
            public <T> void accept(ColumnMetadata.Kind type, String name, Class<T> clazz)
            {
                fields[index] = name;
                types[index] = CLASS_TO_SIMPLE_TYPE_MAP.getOrDefault(clazz, SimpleType.STRING);
            }
        });

        fields[cnt] = ID;
        types[cnt] = SimpleType.INTEGER;

        try
        {
            rowType = new CompositeType(systemView.name(),
                                        systemView.description(),
                                        fields,
                                        fields,
                                        types);
            info = new OpenMBeanInfoSupport(systemView.name(), systemView.description(),
                                            new OpenMBeanAttributeInfo[]{new OpenMBeanAttributeInfoSupport(VIEWS, VIEWS, rowType, true, false, false)},
                                            null,
                                            null,
                                            null);
            sysViewType = new TabularType(systemView.name(), systemView.description(), rowType, new String[]{ ID });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object getAttribute(String attribute)
    {
        if ("MBeanInfo".equals(attribute))
            return getMBeanInfo();

        if (attribute.equals(VIEWS))
            return viewContent();

        throw new IllegalArgumentException("Unknown attribute " + attribute);
    }

    /** {@inheritDoc} */
    @Override
    public void setAttribute(Attribute attribute)
    {
        throw new UnsupportedOperationException("setAttribute is not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public AttributeList setAttributes(AttributeList attributes)
    {
        throw new UnsupportedOperationException("setAttributes is not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException
    {
        if ("getAttribute".equals(actionName))
            return getAttribute((String) params[0]);
        else if ("invoke".equals(actionName))
            return invoke((String) params[0], (Object[]) params[1], (String[]) params[2]);

        throw new UnsupportedOperationException("invoke is not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public AttributeList getAttributes(String[] attributes)
    {
        AttributeList list = new AttributeList();

        for (String attribute : attributes)
            list.add(getAttribute(attribute));

        return list;
    }

    /** {@inheritDoc} */
    @Override
    public MBeanInfo getMBeanInfo()
    {
        return info;
    }

    private TabularDataSupport viewContent()
    {
        TabularDataSupport rows = new TabularDataSupport(sysViewType);
        AttributeToMapVisitor visitor = new AttributeToMapVisitor();

        try
        {
            int idx = 0;

            for (R row : systemView)
            {
                Map<String, Object> data = visitor.getRowData(row, systemView.walker()::visitAll);
                data.put(ID, idx++);
                rows.put(new CompositeDataSupport(rowType, data));
            }
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }

        return rows;
    }

    private static class AttributeToMapVisitor implements SystemViewWalker.AttributeWithValueVisitor
    {
        private Map<String, Object> data;

        public <R> Map<String, Object> getRowData(R row, BiConsumer<R, SystemViewWalker.AttributeWithValueVisitor> action)
        {
            data = new HashMap<>();
            action.accept(row, this);
            return data;
        }

        /** {@inheritDoc} */
        @Override
        public <T> void accept(ColumnMetadata.Kind type, String name, Class<T> clazz, @Nullable T val)
        {
            if (val == null)
                data.put(name, null);
            else if (clazz.isEnum())
                data.put(name, ((Enum<?>) val).name());
            else if (clazz.isAssignableFrom(Class.class))
                data.put(name, ((Class<?>) val).getName());
            else
                data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override
        public void acceptBoolean(ColumnMetadata.Kind type, String name, boolean val)
        {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override
        public void acceptChar(ColumnMetadata.Kind type, String name, char val)
        {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override
        public void acceptByte(ColumnMetadata.Kind type, String name, byte val)
        {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override
        public void acceptShort(ColumnMetadata.Kind type, String name, short val)
        {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override
        public void acceptInt(ColumnMetadata.Kind type, String name, int val)
        {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override
        public void acceptLong(ColumnMetadata.Kind type, String name, long val)
        {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override
        public void acceptFloat(ColumnMetadata.Kind type, String name, float val)
        {
            data.put(name, val);
        }

        /** {@inheritDoc} */
        @Override
        public void acceptDouble(ColumnMetadata.Kind type, String name, double val)
        {
            data.put(name, val);
        }
    }
}
