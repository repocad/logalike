package cern.acet.tracing.output.elasticsearch;

import cern.acet.tracing.util.type.TypeConstraint;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.client.Client;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Static type mapping which does not change.
 */
public class ElasticsearchStaticMapping implements ElasticsearchTypeMapping {

    private final ImmutableMap<String, TypeConstraint<?>> typeMap;

    /**
     * Creates a type mapping with the given type constraints.
     *
     * @param typeMap The type constraints to use in this type mapping.
     */
    public ElasticsearchStaticMapping(ImmutableMap<String, Class<?>> typeMap) {
        this.typeMap = ImmutableMap.<String, TypeConstraint<?>>builder()
                .putAll(
                        typeMap.entrySet()
                                .stream()
                                .collect(Collectors.toMap(Map.Entry::getKey,
                                        tuple -> TypeConstraint.ofClass(tuple.getValue()))))
                .build();
    }

    @Override
    public ImmutableMap<String, TypeConstraint<?>> getTypeMap(Client client) {
        return typeMap;
    }
}
