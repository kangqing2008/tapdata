package io.tapdata.common;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.kit.EmptyKit;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractMqService implements MqService {

    private static final String TAG = AbstractMqService.class.getSimpleName();
    protected final static long MAX_LOAD_TIMEOUT = TimeUnit.SECONDS.toMillis(20L);
    protected final static long SINGLE_MAX_LOAD_TIMEOUT = TimeUnit.SECONDS.toMillis(2L);
    protected final static MqSchemaParser SCHEMA_PARSER = new MqSchemaParser();
    protected final AtomicBoolean consuming = new AtomicBoolean(false);
    protected final static int concurrency = 5;
    protected ExecutorService executorService;

    @Override
    public void close() {
        try {
            consuming.set(false);
            executorService.shutdown();
            Thread.sleep(2000);
        } catch (Exception e) {
            TapLogger.error(TAG, "close service error", e);
        }
    }

}