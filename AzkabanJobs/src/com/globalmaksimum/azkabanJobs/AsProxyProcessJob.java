package com.globalmaksimum.azkabanJobs;

/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import azkaban.Constants;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.jobExecutor.utils.process.AzkabanProcessBuilder;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.SystemMemoryInfo;
import com.google.common.annotations.VisibleForTesting;
import org.apache.log4j.Logger;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;


/**
 * A job that runs a simple unix command
 */
public class AsProxyProcessJob extends AbstractProcessJob {

    public static final String COMMAND = "command";
    public static final String AZKABAN_MEMORY_CHECK = "azkaban.memory.check";
    // Use azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER instead
    @Deprecated
    public static final String NATIVE_LIB_FOLDER = "azkaban.native.lib";
    private static final Duration KILL_TIME = Duration.ofSeconds(30);
    private static final String MEMCHECK_ENABLED = "memCheck.enabled";
    private final CommonMetrics commonMetrics;
    private volatile AzkabanProcess process;
    private volatile boolean killed = false;

    // For testing only. True if the job process exits successfully.
    private volatile boolean success;

    public AsProxyProcessJob(final String jobId, final Props sysProps,
                             final Props jobProps, final Logger log) {
        super(jobId, sysProps, jobProps, log);
        // TODO: reallocf fully guicify CommonMetrics through ProcessJob dependents
        this.commonMetrics = SERVICE_PROVIDER.getInstance(CommonMetrics.class);
    }

    /**
     * Splits the command into a unix like command line structure. Quotes and single quotes are
     * treated as nested strings.
     */
    public static String[] partitionCommandLine(final String command) {
        final ArrayList<String> commands = new ArrayList<>();

        int index = 0;

        StringBuffer buffer = new StringBuffer(command.length());

        boolean isApos = false;
        boolean isQuote = false;
        while (index < command.length()) {
            final char c = command.charAt(index);

            switch (c) {
                case ' ':
                    if (!isQuote && !isApos) {
                        final String arg = buffer.toString();
                        buffer = new StringBuffer(command.length() - index);
                        if (arg.length() > 0) {
                            commands.add(arg);
                        }
                    } else {
                        buffer.append(c);
                    }
                    break;
                case '\'':
                    if (!isQuote) {
                        isApos = !isApos;
                    } else {
                        buffer.append(c);
                    }
                    break;
                case '"':
                    if (!isApos) {
                        isQuote = !isQuote;
                    } else {
                        buffer.append(c);
                    }
                    break;
                default:
                    buffer.append(c);
            }

            index++;
        }

        if (buffer.length() > 0) {
            final String arg = buffer.toString();
            commands.add(arg);
        }

        return commands.toArray(new String[commands.size()]);
    }

    @Override
    public void run() throws Exception {
        try {
            resolveProps();
        } catch (final Exception e) {
            handleError("Bad property definition! " + e.getMessage(), e);
        }

        if (this.sysProps.getBoolean(MEMCHECK_ENABLED, true)
                && this.jobProps.getBoolean(AZKABAN_MEMORY_CHECK, true)) {
            final Pair<Long, Long> memPair = getProcMemoryRequirement();
            final long xms = memPair.getFirst();
            final long xmx = memPair.getSecond();
            // retry backoff in ms
            final String oomMsg = String
                    .format("Cannot request memory (Xms %d kb, Xmx %d kb) from system for job %s",
                            xms, xmx, getId());
            int attempt;
            boolean isMemGranted = true;

            //todo HappyRay: move to proper Guice after this class is refactored.
            final SystemMemoryInfo memInfo = SERVICE_PROVIDER.getInstance(SystemMemoryInfo.class);
            for (attempt = 1; attempt <= Constants.MEMORY_CHECK_RETRY_LIMIT; attempt++) {
                isMemGranted = memInfo.canSystemGrantMemory(xmx);
                if (isMemGranted) {
                    info(String.format("Memory granted for job %s", getId()));
                    if (attempt > 1) {
                        this.commonMetrics.decrementOOMJobWaitCount();
                    }
                    break;
                }
                if (attempt < Constants.MEMORY_CHECK_RETRY_LIMIT) {
                    info(String.format(oomMsg + ", sleep for %s secs and retry, attempt %s of %s",
                            TimeUnit.MILLISECONDS.toSeconds(
                                    Constants.MEMORY_CHECK_INTERVAL_MS), attempt,
                            Constants.MEMORY_CHECK_RETRY_LIMIT));
                    if (attempt == 1) {
                        this.commonMetrics.incrementOOMJobWaitCount();
                    }
                    synchronized (this) {
                        try {
                            this.wait(Constants.MEMORY_CHECK_INTERVAL_MS);
                        } catch (final InterruptedException e) {
                            info(String
                                    .format("Job %s interrupted while waiting for memory check retry", getId()));
                        }
                    }
                    if (this.killed) {
                        this.commonMetrics.decrementOOMJobWaitCount();
                        info(String.format("Job %s was killed while waiting for memory check retry", getId()));
                        return;
                    }
                }
            }

            if (!isMemGranted) {
                this.commonMetrics.decrementOOMJobWaitCount();
                handleError(oomMsg, null);
            }
        }

        List<String> commands = null;
        try {
            commands = getCommandList();
        } catch (final Exception e) {
            handleError("Job set up failed " + e.getCause(), e);
        }

        final long startMs = System.currentTimeMillis();

        if (commands == null) {
            handleError("There are no commands to execute", null);
        }

        info(commands.size() + " commands to execute.");
        final File[] propFiles = initPropsFiles();

        // change krb5ccname env var so that each job execution gets its own cache
        final Map<String, String> envVars = getEnvironmentVariables();

        // we should not execute as user since proxy user rights is required


        for (String command : commands) {
            AzkabanProcessBuilder builder = null;
            info("Command: " + command);
            builder =
                    new AzkabanProcessBuilder(partitionCommandLine(command))
                            .setEnv(envVars).setWorkingDir(getCwd()).setLogger(getLog());


            if (builder.getEnv().size() > 0) {
                info("Environment variables: " + builder.getEnv());
            }
            info("Working directory: " + builder.getWorkingDir());

            // print out the Job properties to the job log.
            this.logJobProperties();

            synchronized (this) {
                // Make sure that checking if the process job is killed and creating an AzkabanProcess
                // object are atomic. The cancel method relies on this to make sure that if this.process is
                // not null, this block of code which includes checking if the job is killed has not been
                // executed yet.
                if (this.killed) {
                    info("The job is killed. Abort. No job process created.");
                    return;
                }
                this.process = builder.build();
            }
            try {
                this.process.run();
                this.success = true;
            } catch (final Throwable e) {
                for (final File file : propFiles) {
                    if (file != null && file.exists()) {
                        file.delete();
                    }
                }
                throw new RuntimeException(e);
            } finally {
                info("Process completed "
                        + (this.success ? "successfully" : "unsuccessfully") + " in "
                        + ((System.currentTimeMillis() - startMs) / 1000) + " seconds.");
            }
        }

        // Get the output properties from this job.
        generateProperties(propFiles[1]);
    }

    /**
     * This is used to get the min/max memory size requirement by processes. SystemMemoryInfo can use
     * the info to determine if the memory request can be fulfilled. For Java process, this should be
     * Xms/Xmx setting.
     *
     * @return pair of min/max memory size
     */
    protected Pair<Long, Long> getProcMemoryRequirement() {
        return new Pair<>(0L, 0L);
    }

    protected void handleError(final String errorMsg, final Exception e) throws Exception {
        error(errorMsg);
        if (e != null) {
            throw new Exception(errorMsg, e);
        } else {
            throw new Exception(errorMsg);
        }
    }

    protected List<String> getCommandList() {
        final List<String> commands = new ArrayList<>();
        commands.add(this.jobProps.getString(COMMAND));
        for (int i = 1; this.jobProps.containsKey(COMMAND + "." + i); i++) {
            commands.add(this.jobProps.getString(COMMAND + "." + i));
        }

        return commands;
    }

    @Override
    public void cancel() throws InterruptedException {
        // in case the job is waiting
        synchronized (this) {
            this.killed = true;
            this.notify();
            if (this.process == null) {
                // The job thread has not checked if the job is killed yet.
                // setting the killed flag should be enough to abort the job.
                // There is no job process to kill.
                return;
            }
        }
        this.process.awaitStartup();
        final boolean processkilled = this.process
                .softKill(KILL_TIME.toMillis(), TimeUnit.MILLISECONDS);
        if (!processkilled) {
            warn("Kill with signal TERM failed. Killing with KILL signal.");
            this.process.hardKill();
        }
    }

    @Override
    public double getProgress() {
        return this.process != null && this.process.isComplete() ? 1.0 : 0.0;
    }

    public int getProcessId() {
        return this.process.getProcessId();
    }

    @VisibleForTesting
    boolean isSuccess() {
        return this.success;
    }

    @VisibleForTesting
    AzkabanProcess getProcess() {
        return this.process;
    }

    public String getPath() {
        return this._jobPath == null ? "" : this._jobPath;
    }
}
