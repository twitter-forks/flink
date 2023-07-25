/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.slowtaskdetector;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SlowTaskDetectorOptions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.IterableUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** The slow task detector which detects slow tasks based on their execution time. */
public class ExecutionTimeBasedSlowTaskDetector implements SlowTaskDetector {

    private final long checkIntervalMillis;

    private final long baselineLowerBoundMillis;

    private final double baselineRatio;

    private final double baselineMultiplier;

    private ScheduledFuture<?> scheduledDetectionFuture;

    private static final Logger LOG =
            LoggerFactory.getLogger(ExecutionTimeBasedSlowTaskDetector.class);

    public ExecutionTimeBasedSlowTaskDetector(Configuration configuration) {
        this.checkIntervalMillis =
                configuration.get(SlowTaskDetectorOptions.CHECK_INTERVAL).toMillis();
        checkArgument(
                this.checkIntervalMillis > 0,
                "The configuration {} should be positive, but is {}.",
                SlowTaskDetectorOptions.CHECK_INTERVAL.key(),
                this.checkIntervalMillis);

        this.baselineLowerBoundMillis =
                configuration
                        .get(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND)
                        .toMillis();
        checkArgument(
                this.baselineLowerBoundMillis >= 0,
                "The configuration {} cannot be negative, but is {}.",
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND.key(),
                this.baselineLowerBoundMillis);

        this.baselineRatio =
                configuration.getDouble(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO);
        checkArgument(
                baselineRatio >= 0 && this.baselineRatio < 1,
                "The configuration {} should be in [0, 1), but is {}.",
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO.key(),
                this.baselineRatio);

        this.baselineMultiplier =
                configuration.getDouble(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER);
        checkArgument(
                baselineMultiplier > 0,
                "The configuration {} should be positive, but is {}.",
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER.key(),
                this.baselineMultiplier);
    }

    @Override
    public void start(
            final ExecutionGraph executionGraph,
            final SlowTaskDetectorListener listener,
            final ComponentMainThreadExecutor mainThreadExecutor) {
        scheduleTask(executionGraph, listener, mainThreadExecutor);
    }

    /** Schedule periodical slow task detection. */
    private void scheduleTask(
            final ExecutionGraph executionGraph,
            final SlowTaskDetectorListener listener,
            final ComponentMainThreadExecutor mainThreadExecutor) {
        this.scheduledDetectionFuture =
                mainThreadExecutor.schedule(
                        () -> {
                            listener.notifySlowTasks(findSlowTasks(executionGraph));
                            scheduleTask(executionGraph, listener, mainThreadExecutor);
                        },
                        checkIntervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    /**
     * Given that the parallelism is N and the ratio is R, define T as the median of the first N*R
     * finished tasks' execution time. The baseline will be T*M, where M is the multiplier. A task
     * will be identified as slow if its execution time is longer than the baseline.
     */
    @VisibleForTesting
    Map<ExecutionVertexID, Collection<ExecutionAttemptID>> findSlowTasks(
            final ExecutionGraph executionGraph) {
        final long currentTimeMillis = System.currentTimeMillis();

        final Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks = new HashMap<>();

        final List<ExecutionJobVertex> jobVerticesToCheck = getJobVerticesToCheck(executionGraph);

        for (ExecutionJobVertex ejv : jobVerticesToCheck) {
            //            final long baseline = getBaseline(ejv, currentTimeMillis);

            for (ExecutionVertex ev : ejv.getTaskVertices()) {
                if (ev.getExecutionState().isTerminal()) {
                    continue;
                }

                //                final List<ExecutionAttemptID> slowExecutions =
                //                        findExecutionsExceedingBaseline(
                //                                ev.getCurrentExecutions(), baseline,
                // currentTimeMillis);
                final List<ExecutionAttemptID> slowExecutions =
                        findSlowExecutionsByThroughput(
                                ev.getCurrentExecutions(), currentTimeMillis);
                LOG.info(
                        "Slow executions for job vertex {} - {}",
                        ev.getJobVertex().getName(),
                        slowExecutions);

                if (!slowExecutions.isEmpty()) {
                    slowTasks.put(ev.getID(), slowExecutions);
                }
            }
        }

        return slowTasks;
    }

    private List<ExecutionJobVertex> getJobVerticesToCheck(final ExecutionGraph executionGraph) {
        return IterableUtils.toStream(executionGraph.getVerticesTopologically())
                .filter(ExecutionJobVertex::isInitialized)
                .filter(ejv -> ejv.getAggregateState() != ExecutionState.FINISHED)
                //                .filter(ejv -> getFinishedRatio(ejv) >= baselineRatio)
                .collect(Collectors.toList());
    }

    private double getFinishedRatio(final ExecutionJobVertex executionJobVertex) {
        checkState(executionJobVertex.getTaskVertices().length > 0);
        long finishedCount =
                Arrays.stream(executionJobVertex.getTaskVertices())
                        .filter(ev -> ev.getExecutionState() == ExecutionState.FINISHED)
                        .count();
        return (double) finishedCount / executionJobVertex.getTaskVertices().length;
    }

    private long getBaseline(
            final ExecutionJobVertex executionJobVertex, final long currentTimeMillis) {
        final long executionTimeMedian =
                calculateFinishedTaskExecutionTimeMedian(executionJobVertex, currentTimeMillis);
        return (long) Math.max(baselineLowerBoundMillis, executionTimeMedian * baselineMultiplier);
    }

    private long calculateFinishedTaskExecutionTimeMedian(
            final ExecutionJobVertex executionJobVertex, final long currentTime) {

        final int baselineExecutionCount =
                (int) Math.round(executionJobVertex.getParallelism() * baselineRatio);

        if (baselineExecutionCount == 0) {
            return 0;
        }

        final List<Execution> finishedExecutions =
                Arrays.stream(executionJobVertex.getTaskVertices())
                        .flatMap(ev -> ev.getCurrentExecutions().stream())
                        .filter(e -> e.getState() == ExecutionState.FINISHED)
                        .collect(Collectors.toList());

        checkState(finishedExecutions.size() >= baselineExecutionCount);

        final List<Long> firstFinishedExecutions =
                finishedExecutions.stream()
                        .map(e -> getExecutionTime(e, currentTime))
                        .sorted()
                        .limit(baselineExecutionCount)
                        .collect(Collectors.toList());

        return firstFinishedExecutions.get(baselineExecutionCount / 2);
    }

    private List<ExecutionAttemptID> findSlowExecutionsByThroughput(
            Collection<Execution> executions, long currentTimeMillis) {
        List<Pair<Execution, Double>> sortedByThroughput =
                executions.stream()
                        .filter(
                                e ->
                                        !e.getState().isTerminal()
                                                && e.getState() != ExecutionState.CANCELING)
                        .filter(
                                e ->
                                        getExecutionTime(e, currentTimeMillis)
                                                >= baselineLowerBoundMillis)
                        .filter(e -> e.getIOMetrics().getNumRecordsIn() > 0)
                        .filter(e -> e.getIOMetrics().getNumRecordsOut() > 0)
                        .map(e -> Pair.of(e, computeThroughput(e, currentTimeMillis)))
                        .sorted(Comparator.comparingDouble(e -> e.getRight()))
                        .collect(Collectors.toList());
        LOG.debug("Executions sorted by throughput- {}", sortedByThroughput);
        if (sortedByThroughput.isEmpty()) {
            return Collections.emptyList();
        } else {
            Pair<Execution, Double> maxThroughputExecutionAndTime =
                    sortedByThroughput.get(sortedByThroughput.size() - 1);
            LOG.info("Fastest execution - {}", maxThroughputExecutionAndTime);
            double baselineThroughput =
                    maxThroughputExecutionAndTime.getRight() * baselineMultiplier;
            List<ExecutionAttemptID> slowTasks = new ArrayList<>();
            int index = 0;
            while (index < sortedByThroughput.size()
                    && sortedByThroughput.get(index).getRight() < baselineThroughput) {
                slowTasks.add(sortedByThroughput.get(index).getLeft().getAttemptId());
                index++;
            }
            return slowTasks;
        }
    }

    private double computeThroughput(Execution e, long currentTimeMills) {
        if (e.getIOMetrics().getNumBytesIn() == 0) {
            return 0;
        } else {
            return e.getIOMetrics().getNumBytesOut()
                    * 1.0
                    / e.getIOMetrics().getNumBytesIn()
                    / getExecutionTime(e, currentTimeMills);
        }
    }

    private List<ExecutionAttemptID> findExecutionsExceedingBaseline(
            Collection<Execution> executions, long baseline, long currentTimeMillis) {
        return executions.stream()
                .filter(e -> !e.getState().isTerminal() && e.getState() != ExecutionState.CANCELING)
                .filter(e -> getExecutionTime(e, currentTimeMillis) >= baseline)
                .map(Execution::getAttemptId)
                .collect(Collectors.toList());
    }

    private long getExecutionTime(final Execution execution, final long currentTime) {
        final long deployingTimestamp = execution.getStateTimestamp(ExecutionState.DEPLOYING);
        if (deployingTimestamp == 0) {
            return 0;
        }

        if (execution.getState() == ExecutionState.FINISHED) {
            return execution.getStateTimestamp(ExecutionState.FINISHED) - deployingTimestamp;
        } else {
            return currentTime - deployingTimestamp;
        }
    }

    @Override
    public void stop() {
        if (scheduledDetectionFuture != null) {
            scheduledDetectionFuture.cancel(false);
        }
    }
}
