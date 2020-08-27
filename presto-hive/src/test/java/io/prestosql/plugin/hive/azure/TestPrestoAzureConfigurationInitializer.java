/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive.azure;

import io.airlift.configuration.ConfigurationMetadata;
import io.airlift.configuration.ConfigurationMetadata.AttributeMetadata;
import io.airlift.configuration.ConfigurationMetadata.InjectionPointMetaData;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.combinations;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;

public class TestPrestoAzureConfigurationInitializer
{
    /**
     * Map from presto config property names to the corresponding
     * {@link HiveAzureConfig} setter method. Use via {@link #toConfig}.
     */
    private static final Map<String, Method> PROPERTY_SETTERS =
            ConfigurationMetadata.getValidConfigurationMetadata(HiveAzureConfig.class)
                    .getAttributes()
                    .values()
                    .stream()
                    .map(AttributeMetadata::getInjectionPoint)
                    .collect(Collectors.toMap(
                            InjectionPointMetaData::getProperty,
                            InjectionPointMetaData::getSetter));

    private static final Set<String> abfsAccessKey = Set.of(
            "hive.azure.abfs-storage-account",
            "hive.azure.abfs-access-key");

    private static final Set<String> abfsOAuth = Set.of(
            "hive.azure.abfs.oauth-client-endpoint",
            "hive.azure.abfs.oauth-client-id",
            "hive.azure.abfs.oauth-client-secret");

    private static final Set<Set<String>> propertyGroups = Set.of(
            abfsAccessKey,
            abfsOAuth,
            Set.of(
                    "hive.azure.wasb-storage-account",
                    "hive.azure.wasb-access-key"),
            Set.of(
                    "hive.azure.adl-client-id",
                    "hive.azure.adl-credential",
                    "hive.azure.adl-refresh-url"));

    // Pairs of groups that can not be given together
    private static final Set<Set<Set<String>>> exclusivePropertyGroups = Set.of(
            Set.of(abfsAccessKey, abfsOAuth));

    @DataProvider(parallel = true)
    public Iterator<Object[]> propertyGroups()
    {
        return propertyGroups.stream().map(p -> new Object[]{p}).iterator();
    }

    @Test(dataProvider = "propertyGroups")
    public void testPropertyGroups(Set<String> properties)
    {
        var config = toConfig(properties);
        try {
            new PrestoAzureConfigurationInitializer(config);
        }
        catch (IllegalArgumentException e) {
            fail("Expected configuration to be valid, but got error", e);
        }
    }

    // Provides property groups with each element removed in turn
    @DataProvider(parallel = true)
    public Iterator<Object[]> missingProperties()
    {
        return propertyGroups.stream()
                .flatMap(properties -> combinations(properties, properties.size() - 1).stream())
                .map(p -> new Object[]{p})
                .iterator();
    }

    @Test(dataProvider = "missingProperties")
    public void testMissingProperty(Set<String> properties)
    {
        var config = toConfig(properties);
        assertThrows(
                IllegalArgumentException.class,
                () -> new PrestoAzureConfigurationInitializer(config));
    }

    @DataProvider(parallel = true) // Set<String>, Set<String>
    public Iterator<Object[]> exclusiveProperties()
    {
        return exclusivePropertyGroups.stream().map(Set::toArray).iterator();
    }

    @Test(dataProvider = "exclusiveProperties")
    public void testExclusiveProperties(Set<String> group1, Set<String> group2)
    {
        var config = toConfig(group1, group2);
        assertThrows(() -> new PrestoAzureConfigurationInitializer(config));
    }

    /**
     * Combine sets of properties into a {@code HiveAzureConfig} with all given
     * properties set to some value.
     */
    @SafeVarargs
    private static HiveAzureConfig toConfig(Iterable<String>... propertySets)
    {
        var config = new HiveAzureConfig();
        for (Iterable<String> properties : propertySets) {
            for (String property : properties) {
                if (!PROPERTY_SETTERS.containsKey(property)) {
                    throw new IllegalArgumentException("Testing invalid config property: " + property);
                }

                try {
                    PROPERTY_SETTERS.get(property).invoke(config, "test value");
                }
                catch (ReflectiveOperationException e) {
                    throw new IllegalArgumentException("Bad call to config setter in test", e);
                }
            }
        }
        return config;
    }
}
