package com.hartwig.miniwe;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public final class ThreadUtil {
    public static ExecutorService createExecutorService(int nMaxThreads, String nameTemplate) {
        return new ThreadPoolExecutor(1,
                nMaxThreads,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadFactoryBuilder().setNameFormat(nameTemplate).build());
    }
}
