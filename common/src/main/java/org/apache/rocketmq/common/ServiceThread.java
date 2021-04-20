/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 具备定时执行能力和唤醒能力
 */
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 内置执行任务的线程
     */
    private Thread thread;
    /**
     * 可复用的计数器，基于AQS实现，对比JDK的 CountDownLatch，实现了reset方法重置内部计量到初始值
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    /**
     * 线程状态标识：当false时代表处于等待阻塞状态；true时代表工作状态；
     * 根据变量名语义：是否被通知。true代表被通知过，一般对应活跃状态；false代表没被通知，一般对应阻塞状态
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 停止标识，代表线程消亡
     */
    protected volatile boolean stopped = false;
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    /**
     * 控制了启动了状态。同时只能启动一次。
     * 在线程被stop后消亡时，可以继续启动一个新的线程
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public abstract String getServiceName();

    /**
     * 内部 thread 可以重复赋值，由此可见可以重复启动；started控制了启动了状态。同时只能启动一次。
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    /**
     * 唤醒线程，如果其处于等待状态
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 注意 hasNotified的作用。如果之前已经是活跃状态了，那么这里会变成阻塞状态，直接返回；那么执行这段代码的线程就不用走到后续的await逻辑里进行等待了。
     * 只要保证执行此方法的一直是服务内部的线程，其作用就变成了如果是活跃状态的话，就不用等待直接返回执行后面的逻辑，同时把通知的标志位置为未通知，下次进来就需要等待了。
     */
    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //entry to wait
        waitPoint.reset(); // 重置状态

        try {
            // 阻塞等待指定的时间。要么被唤醒，要么时间到达
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
