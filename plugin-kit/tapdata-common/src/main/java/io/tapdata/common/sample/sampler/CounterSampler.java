package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.concurrent.atomic.LongAdder;

/**
 * Auto inc counter.
 * The counter will not be cleared when upload.
 */
public class CounterSampler implements Sampler {
    private final LongAdder counter = new LongAdder();

    public CounterSampler() {
    }

    public CounterSampler(long initialValue) {
        counter.add(initialValue);
    }

    public void inc() {
        counter.increment();
    }
    public void inc(long value) {
        counter.add(value);
    }

    @Override
    public Number value() {
        return counter.longValue();
    }
}
