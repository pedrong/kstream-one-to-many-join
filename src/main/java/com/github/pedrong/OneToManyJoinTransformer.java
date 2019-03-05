package com.github.pedrong;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Custom transformer which allows a one-to-many join with a {@link GlobalKTable}.
 *
 * @param <TK> Key type for the mapping data in the KTable.
 * @param <TV> Value type for the mapping data in the KTable.
 * @param <K> Key for the processing record.
 * @param <V> Value for the processing record.
 */
public class OneToManyJoinTransformer<TK, TV, K, V> implements Transformer<K, V, KeyValue<K, V>> {

    private final GlobalKTable<TK, TV> globalKTable;
    private final KeyValueMapper<K, V, Collection<TK>> keyValueMapper;
    private final ValueJoiner<V, List<TV>, V> valueJoiner;
    private final boolean leftJoin;
    private final boolean strictResultPosition;

    protected ProcessorContext context;
    protected KeyValueStore<TK, TV> stateStore;

    /**
     * Custom transformer which allows a one-to-many join with a {@link GlobalKTable}.
     *
     * @param globalKTable Table containing the data to be joined.
     * @param keyValueMapper Function which must return a collection of keys to be joined.
     * @param valueJoiner Function that must join the processing record to the {@link List<TV>} of found values from the
     *                    globalKTable.
     * @param leftJoin if <code>true</code> the processor will return the processing record even if one or more keys
     *                 could not be found in the globalKTable. If <code>false</code>, if a single entry could not be
     *                 found in the globalKTable the processing record will be discard and will not continue down the
     *                 stream.
     */
    public OneToManyJoinTransformer(GlobalKTable<TK, TV> globalKTable, KeyValueMapper<K, V, Collection<TK>> keyValueMapper, ValueJoiner<V, List<TV>, V> valueJoiner, boolean leftJoin, boolean strictResultPosition) {
        Objects.requireNonNull(globalKTable);
        Objects.requireNonNull(keyValueMapper);
        Objects.requireNonNull(valueJoiner);

        this.globalKTable = globalKTable;
        this.keyValueMapper = keyValueMapper;
        this.valueJoiner = valueJoiner;
        this.leftJoin = leftJoin;
        this.strictResultPosition = strictResultPosition;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        final String storeName = globalKTable.queryableStoreName();

        if (storeName == null)
            throw new InvalidStateStoreException("GlobalKTable does not have a queryable store");

        this.stateStore = (KeyValueStore) context.getStateStore(storeName);

        if (stateStore == null)
            throw new InvalidStateStoreException(String.format("Could not find GlobalKTable state store %s from the context.", storeName));
    }

    @Override
    public KeyValue<K, V> transform(K key, V value) {

        final Collection<TK> keysList = keyValueMapper.apply(key, value);

        // If no keys were provided
        if (keysList == null || keysList.isEmpty()) {
            if (!leftJoin) {
                return null;
            } else {
                return KeyValue.pair(key, value);
            }
        }

        final List<TV> valuesList = new ArrayList<>(keysList.size());

        // For each key it does a lookup in the state store
        keysList.forEach(keyEntry -> {
            // tries to find in table state store
            final TV foundValue = (keyEntry != null) ? stateStore.get(keyEntry) : null;
            // Adds to result list if found or adds null if it is a leftJoin with strictResultPosition
            if (foundValue != null || (leftJoin && strictResultPosition))
                valuesList.add(foundValue);
        });

        // If values are missing and it is not a left join, then return nothing and hence stop processing
        if (!leftJoin && valuesList.size() < keysList.size())
            return null;

        // Invoke joiner function which will create a new value
        final V newValue = valueJoiner.apply(value, valuesList);

        return KeyValue.pair(key, newValue);
    }

    @Override
    public void close() {}

}
