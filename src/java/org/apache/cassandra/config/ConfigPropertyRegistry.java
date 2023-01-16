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

package org.apache.cassandra.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;
import org.yaml.snakeyaml.introspector.FieldProperty;
import org.yaml.snakeyaml.introspector.Property;

import org.apache.cassandra.config.sysview.ConfigPropertyViewRow;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.sysview.SystemViewRegistry;
import org.apache.cassandra.sysview.walker.ConfigPropertySystemViewWalker;

/**
 * This is a simple configuration proerty registry that stores all the {@link Config} properties.
 * Basically it is simplified verstion of {@code Configuration} class of the commons-configuration
 * library to avoid adding an extra dependency for now.
 * TODO Move to another package.
 */
public class ConfigPropertyRegistry
{
    public static final ConfigPropertyRegistry instance = new ConfigPropertyRegistry();
    public static final String CONFIG_PROPERTY_VIEW_NAME = "ConfigurationSettings";
    public static final String CONFIG_PROPERTY_VIEW_DESC = "The view on internal configuration properties";
    private static final Map<String, String> BACKWARDS_COMPATABLE_NAMES = ImmutableMap.copyOf(getBackwardsCompatableNames());
    private final Map<String, Property> PROPERTIES;
    private final Config config;

    protected ConfigPropertyRegistry()
    {
        this.PROPERTIES = ImmutableMap.copyOf(getProperties());
        this.config = DatabaseDescriptor.getRawConfig();

        SystemViewRegistry.instance.registerView(CONFIG_PROPERTY_VIEW_NAME, CONFIG_PROPERTY_VIEW_DESC,
                                                 new ConfigPropertySystemViewWalker(), PROPERTIES.entrySet(),
                                                 entry -> new ConfigPropertyViewRow(entry, instance::getValue));
    }

    public String getValue(String name)
    {
        Object value = PROPERTIES.get(name).get(config);
        return value == null ? "" : value.toString();
    }

    public void setPropetry(String name, Object value) {
        // TODO we need value converter here that supports Config fields formatта
        // TODO CommitLog should register his setters that it is intended to change
        Property property = PROPERTIES.get(name);
        assert property instanceof FieldProperty;

        try
        {
            if (property.isWritable())
                property.set(Config.class, value);
        }
        catch (Exception e) {
            throw new ConfigurationException(String.format("Error updating property with name: %s", name), e);
        }
    }

    public void registerPropertySetter(String name) {

    }

    private static Map<String, Property> getProperties()
    {
        Loader loader = Properties.defaultLoader();

        Map<String, Property> properties = loader.flatten(Config.class);
        // only handling top-level replacements for now, previous logic was only top level so not a regression
        Map<String, Replacement> replacements = Replacements.getNameReplacements(Config.class).get(Config.class);
        if (replacements != null)
        {
            for (Replacement r : replacements.values())
            {
                Property latest = properties.get(r.newName);
                assert latest != null : "Unable to find replacement new name: " + r.newName;
                Property conflict = properties.put(r.oldName, r.toProperty(latest));
                // some configs kept the same name, but changed the type, if this is detected then rely on the replaced property
                assert conflict == null || r.oldName.equals(r.newName) : String.format("New property %s attempted to replace %s, but this property already exists", latest.getName(), conflict.getName());
            }
        }

        for (Map.Entry<String, String> e : BACKWARDS_COMPATABLE_NAMES.entrySet())
        {
            String oldName = e.getKey();
            if (properties.containsKey(oldName))
                throw new AssertionError("Name " + oldName + " is present in Config, this adds a conflict as this name had a different meaning.");
            String newName = e.getValue();
            Property prop = Objects.requireNonNull(properties.get(newName), newName + " cant be found for " + oldName);
            properties.put(oldName, Properties.rename(oldName, prop));
        }
        return properties;
    }

    /**
     * settings table was released in 4.0 and attempted to support nested properties for a few hand selected properties.
     * The issue is that 4.0 used '_' to seperate the names, which makes it hard to map back to the yaml names; to solve
     * this 4.1+ uses '.' to avoid possible conflicts, this class provides mappings from old names to the '.' names.
     *
     * There were a handle full of properties which had custom names, names not present in the yaml, this map also
     * fixes this and returns the proper (what is accessable via yaml) names.
     */
    private static Map<String, String> getBackwardsCompatableNames()
    {
        Map<String, String> names = new HashMap<>();
        // Names that dont match yaml
        names.put("audit_logging_options_logger", "audit_logging_options.logger.class_name");
        names.put("server_encryption_options_client_auth", "server_encryption_options.require_client_auth");
        names.put("server_encryption_options_endpoint_verification", "server_encryption_options.require_endpoint_verification");
        names.put("server_encryption_options_legacy_ssl_storage_port", "server_encryption_options.legacy_ssl_storage_port_enabled");
        names.put("server_encryption_options_protocol", "server_encryption_options.accepted_protocols");

        // matching names
        names.put("audit_logging_options_audit_logs_dir", "audit_logging_options.audit_logs_dir");
        names.put("audit_logging_options_enabled", "audit_logging_options.enabled");
        names.put("audit_logging_options_excluded_categories", "audit_logging_options.excluded_categories");
        names.put("audit_logging_options_excluded_keyspaces", "audit_logging_options.excluded_keyspaces");
        names.put("audit_logging_options_excluded_users", "audit_logging_options.excluded_users");
        names.put("audit_logging_options_included_categories", "audit_logging_options.included_categories");
        names.put("audit_logging_options_included_keyspaces", "audit_logging_options.included_keyspaces");
        names.put("audit_logging_options_included_users", "audit_logging_options.included_users");
        names.put("server_encryption_options_algorithm", "server_encryption_options.algorithm");
        names.put("server_encryption_options_cipher_suites", "server_encryption_options.cipher_suites");
        names.put("server_encryption_options_enabled", "server_encryption_options.enabled");
        names.put("server_encryption_options_internode_encryption", "server_encryption_options.internode_encryption");
        names.put("server_encryption_options_optional", "server_encryption_options.optional");
        names.put("transparent_data_encryption_options_chunk_length_kb", "transparent_data_encryption_options.chunk_length_kb");
        names.put("transparent_data_encryption_options_cipher", "transparent_data_encryption_options.cipher");
        names.put("transparent_data_encryption_options_enabled", "transparent_data_encryption_options.enabled");
        names.put("transparent_data_encryption_options_iv_length", "transparent_data_encryption_options.iv_length");

        return names;
    }
}
