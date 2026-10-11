package com.github.sabomichal.pgadvisory;

import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Maps a lock key to the text stored in the lock table and to the id passed to {@code pg_advisory_lock}.
 *
 * @param <K> type of the lock key
 */
public sealed interface KeyMapper<K> {

    /** Text form of the key, stored in the {@code lock_key} column. Must be stable and unique per key. */
    String keyText(K key);

    /** Advisory lock id of the key whose lock table row has id {@code rowId}. */
    long lockId(K key, long rowId);

    /**
     * Identity of the advisory lock as far as it is known before any database call: keys with equal local keys
     * exclude each other within one instance ({@code HeldLocally}), like they do across instances ({@code Busy}).
     */
    Object localKey(K key);

    /** Collision free: the advisory lock id is the id of the lock table row. Keys are stored by {@code toString()}. */
    static <K> KeyMapper<K> registry() {
        return registry(Object::toString);
    }

    /** Collision free: the advisory lock id is the id of the lock table row. */
    static <K> KeyMapper<K> registry(Function<K, String> keyText) {
        return new Registry<>(keyText);
    }

    /**
     * The advisory lock id is computed from the key. Keys mapping to the same id only contend
     * with each other, within one instance as well as across instances; safety is not affected.
     */
    static <K> KeyMapper<K> direct(ToLongFunction<K> lockId, Function<K, String> keyText) {
        return new Direct<>(lockId, keyText);
    }

    record Registry<K>(Function<K, String> keyTextFunction) implements KeyMapper<K> {

        @Override
        public String keyText(K key) {
            return keyTextFunction.apply(key);
        }

        @Override
        public long lockId(K key, long rowId) {
            return rowId;
        }

        /** The text: it maps one to one to the row and so to the lock id. */
        @Override
        public Object localKey(K key) {
            return keyText(key);
        }
    }

    record Direct<K>(ToLongFunction<K> lockIdFunction, Function<K, String> keyTextFunction) implements KeyMapper<K> {

        @Override
        public String keyText(K key) {
            return keyTextFunction.apply(key);
        }

        @Override
        public long lockId(K key, long rowId) {
            return lockIdFunction.applyAsLong(key);
        }

        /** The lock id: different keys may map to it. */
        @Override
        public Object localKey(K key) {
            return lockIdFunction.applyAsLong(key);
        }
    }
}
