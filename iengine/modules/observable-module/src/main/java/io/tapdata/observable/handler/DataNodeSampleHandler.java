package io.tapdata.observable.handler;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.common.executor.ExecutorsManager;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.NumberSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.index.TapDeleteIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckFunction;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Dexter
 */
public class DataNodeSampleHandler extends AbstractNodeSampleHandler {
    private final Logger logger = LogManager.getLogger(DataNodeSampleHandler.class);
    private static final String TAG = DataNodeSampleHandler.class.getSimpleName();
    public DataNodeSampleHandler(SubTaskDto task) {
        super(task);
    }

    private final Map<String, SampleCollector> collectors = new HashMap<>();

    // event related metrics
    private final Map<String, CounterSampler> insertCounters = new HashMap<>();
    private final Map<String, CounterSampler> updateCounters = new HashMap<>();
    private final Map<String, CounterSampler> deleteCounters = new HashMap<>();
    private final Map<String, CounterSampler> ddlCounters = new HashMap<>();
    private final Map<String, CounterSampler> othersCounters = new HashMap<>();

    private final Map<String, SpeedSampler> qpsSpeeds = new HashMap<>();

    private final Map<String, AverageSampler> timeCostAverages = new HashMap<>();

    private final Map<String, NumberSampler<Long>>  currentEventTimestamps = new HashMap<>();


    public void init(Node<?> node, String associateId) {
        Map<String, String> tags = nodeTags(node);

        // TODO(dexter): use the initial value from db
        String nodeId = node.getId();
        SampleCollector collector = CollectorFactory.getInstance().getSampleCollectorByTags("nodeSamplers", tags);
        collectors.put(nodeId, collector);

        // only data nodes have events metrics
        insertCounters.put(nodeId, collector.getCounterSampler("insertTotal"));
        updateCounters.put(nodeId, collector.getCounterSampler("updateTotal"));
        deleteCounters.put(nodeId, collector.getCounterSampler("deleteTotal"));
        ddlCounters.put(nodeId, collector.getCounterSampler("ddlTotal"));
        othersCounters.put(nodeId, collector.getCounterSampler("othersTotal"));

        // data node only has one qps, input qps for target node while output qps for source node
        qpsSpeeds.put(nodeId, collector.getSpeedSampler("qps"));

        timeCostAverages.put(nodeId, collector.getAverageSampler("timeCostAvg"));

        currentEventTimestamps.put(nodeId, collector.getNumberCollector("currentEventTimestamp", Long.class));

        // run health check
        runHealthCheck(nodeId, associateId);

    }

    public void close(Node<?> node) {
        String nodeId = node.getId();
        Optional.ofNullable(collectors.get(nodeId)).ifPresent(collector -> {
            CollectorFactory.getInstance().removeSampleCollectorByTags(collector.tags());
        });
    }

    private final Map<String, Long> batchAcceptLastTs = new HashMap<>();
    public void handleBatchReadStart(String nodeId, Long startAt) {
        batchAcceptLastTs.put(nodeId, startAt);
    }

    public void handleBatchReadAccept(String nodeId, Long acceptTime, long size) {
        Optional.ofNullable(timeCostAverages.get(nodeId)).ifPresent(average -> {
            average.add(size, acceptTime - batchAcceptLastTs.get(nodeId));
            batchAcceptLastTs.put(nodeId, acceptTime);
        });
        // batch read only has insert events
        Optional.ofNullable(qpsSpeeds.get(nodeId)).ifPresent(speed -> speed.add(size));
        Optional.ofNullable(insertCounters.get(nodeId)).ifPresent(counter -> counter.inc(size));
    }

    private final Map<String, Long> streamAcceptLastTs = new HashMap<>();
    public void handleStreamReadStreamStart(String nodeId, Long startAt) {
        streamAcceptLastTs.put(nodeId, startAt);
    }

    public void handleStreamReadAccept(String nodeId, Long acceptTime, List<TapdataEvent> events) {
        Optional.ofNullable(timeCostAverages.get(nodeId)).ifPresent(average -> {
            average.add(events.size(), acceptTime - streamAcceptLastTs.get(nodeId));
            streamAcceptLastTs.put(nodeId, acceptTime);
        });
        Optional.ofNullable(qpsSpeeds.get(nodeId)).ifPresent(speed -> speed.add(events.size()));
        for (TapdataEvent tapdataEvent : events) {
            Optional.ofNullable(currentEventTimestamps.get(nodeId)).ifPresent(
                    sampler -> sampler.setValue(tapdataEvent.getTapEvent().getTime())
            );
            int type = 0;
            if (tapdataEvent.getTapEvent() != null) {
                type = tapdataEvent.getTapEvent().getType();
            }
            switch (type) {
                case TapInsertRecordEvent.TYPE:
                    Optional.ofNullable(insertCounters.get(nodeId)).ifPresent(CounterSampler::inc);
                    break;
                case TapDeleteRecordEvent.TYPE:
                    Optional.ofNullable(deleteCounters.get(nodeId)).ifPresent(CounterSampler::inc);
                    break;
                case TapUpdateRecordEvent.TYPE:
                    Optional.ofNullable(updateCounters.get(nodeId)).ifPresent(CounterSampler::inc);
                    break;
                case TapDeleteIndexEvent.TYPE:
                case TapCreateIndexEvent.TYPE:
                case TapAlterDatabaseTimezoneEvent.TYPE:
                case TapAlterFieldAttributesEvent.TYPE:
                case TapAlterFieldNameEvent.TYPE:
                case TapAlterFieldPrimaryKeyEvent.TYPE:
                case TapAlterTableCharsetEvent.TYPE:
                case TapClearTableEvent.TYPE:
                case TapCreateTableEvent.TYPE:
                case TapDropFieldEvent.TYPE:
                case TapDropTableEvent.TYPE:
                case TapNewFieldEvent.TYPE:
                case TapRenameTableEvent.TYPE:
                    Optional.ofNullable(ddlCounters.get(nodeId)).ifPresent(CounterSampler::inc);
                    break;
                default:
                    Optional.ofNullable(othersCounters.get(nodeId)).ifPresent(CounterSampler::inc);
            }
        }
    }

    private final Map<String, Long> writeRecordAcceptLastTs = new HashMap<>();

    public void handleWriteRecordStart(String nodeId, Long startAt) {
        writeRecordAcceptLastTs.put(nodeId, startAt);
    }

    public void handleWriteRecordAccept(String nodeId, Long acceptTime, WriteListResult<TapRecordEvent> result) {
        long inserted = result.getInsertedCount();
        long updated = result.getModifiedCount();
        long deleted = result.getRemovedCount();
        long total = inserted + updated + deleted;

        Optional.ofNullable(insertCounters.get(nodeId)).ifPresent(counter -> counter.inc(inserted));
        Optional.ofNullable(updateCounters.get(nodeId)).ifPresent(counter -> counter.inc(updated));
        Optional.ofNullable(deleteCounters.get(nodeId)).ifPresent(counter -> counter.inc(deleted));

        Optional.ofNullable(qpsSpeeds.get(nodeId)).ifPresent(speed -> speed.add(total));
        Optional.ofNullable(timeCostAverages.get(nodeId)).ifPresent(average -> {
            average.add(total, acceptTime - writeRecordAcceptLastTs.get(nodeId));
            writeRecordAcceptLastTs.put(nodeId, acceptTime);
        });

    }

    private static final int PERIOD_SECOND = 5;
    private ScheduledExecutorService scheduleExecutorService;
    private final Map<String, ConnectorNode> connectorNodeMap = new HashMap<>();
    private Map<String, NumberSampler<Long>> tcpPingNumbers = new HashMap<>();
    private Map<String, NumberSampler<Long>> connectPingNumbers = new HashMap<>();
    private void runHealthCheck(String nodeId, String associateId) {
        tcpPingNumbers.put(nodeId, collectors.get(nodeId).getNumberCollector("tcpPing", Long.class));
        connectPingNumbers.put(nodeId, collectors.get(nodeId).getNumberCollector("connectPing", Long.class));

        connectorNodeMap.putIfAbsent(nodeId, ConnectorNodeService.getInstance().getConnectorNode(associateId));
        // start thread to get the tcp ping and connect ping
        if (null == scheduleExecutorService) {
            String name = String.format("Task data node health check %s-%s", task.getName(), task.getId());
            scheduleExecutorService = ExecutorsManager.getInstance().newSingleThreadScheduledExecutor(name);
            scheduleExecutorService.scheduleAtFixedRate(() -> {
                for (String id : connectorNodeMap.keySet()) {
                    ConnectorNode connectorNode = connectorNodeMap.get(id);
                    ConnectionCheckFunction function = connectorNode.getConnectorFunctions().getConnectionCheckFunction();
                    if (null == function) {
                        continue;
                    }
                    PDKInvocationMonitor.invoke(connectorNode, PDKMethod.CONNECTION_CHECK, () -> {
                        function.check(
                                connectorNode.getConnectorContext(),
                                Arrays.asList(
                                        ConnectionCheckItem.ITEM_PING,
                                        ConnectionCheckItem.ITEM_CONNECTION
                                ),
                                item -> {
                                    NumberSampler<Long> sampler = null;
                                    switch (item.getItem()) {
                                        case ConnectionCheckItem.ITEM_PING:
                                            sampler = tcpPingNumbers.get(id);
                                            break;
                                        case ConnectionCheckItem.ITEM_CONNECTION:
                                            sampler = connectPingNumbers.get(id);
                                            break;
                                    }
                                    Optional.ofNullable(sampler).ifPresent(s -> {
                                        Long value;
                                        // connection check failed, use -1 as value
                                        if (item.getResult() == ConnectionCheckItem.RESULT_FAILED) {
                                            value = -1L;
                                        } else {
                                            value = item.getTakes();
                                        }
                                        s.setValue(Optional.ofNullable(value).orElse(-1L));
                                    });
                                }
                        );
                    }, TAG);
                }
            }, 0L, PERIOD_SECOND, TimeUnit.SECONDS);
        }
    }
}
