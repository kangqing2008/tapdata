package io.tapdata.observable.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.CounterSampler;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Dexter
 */
public class TableSampleHandler extends AbstractHandler {
    public TableSampleHandler(SubTaskDto task) {
        super(task);
    }

    public Map<String, String> tableTags(String table) {
        Map<String, String> tags = baseTags(SAMPLE_TYPE_TABLE);
        tags.put("table", table);

        return tags;
    }

    private final Map<String, Map<String, SampleCollector>> collectors = new HashMap<>();

    private final Map<String, Map<String, CounterSampler>> snapshotTotals = new HashMap<>();

    public void init(Node<?> node, Map<String, Long> tableCountMap) {
        String nodeId = node.getId();
        for(Map.Entry<String, Long> entry : tableCountMap.entrySet()) {
            String table = entry.getKey();
            SampleCollector collector = CollectorFactory.getInstance()
                    .getSampleCollectorByTags("tableSamplers", tableTags(table));
            collectors.putIfAbsent(nodeId, new HashMap<>());
            collectors.get(nodeId).put(table, collector);

            snapshotTotals.putIfAbsent(nodeId, new HashMap<>());
            snapshotTotals.get(nodeId).put(table, collector.getCounterSampler("snapshotTotal", entry.getValue()));

            snapshotInsertTotals.putIfAbsent(nodeId, new HashMap<>());
            snapshotInsertTotals.get(nodeId).put(table, collector.getCounterSampler("snapshotInsertTotal"));
        }
    }

    public void close(Node<?> node) {
        String nodeId = node.getId();
        Map<String, SampleCollector> tableCollectors = collectors.get(nodeId);
        if (null == tableCollectors || tableCollectors.isEmpty()) {
            return;
        }
        for(SampleCollector collector : tableCollectors.values()) {
            CollectorFactory.getInstance().removeSampleCollectorByTags(collector.tags());
        }
    }

    private final Map<String, Map<String, CounterSampler>> snapshotInsertTotals = new HashMap<>();

    public void incrTableSnapshotInsertTotal(String nodeId, String table) {
        incrTableSnapshotInsertTotal(nodeId, table, 1L);
    }

    public void incrTableSnapshotInsertTotal(String nodeId, String table, Long value) {
        if (snapshotInsertTotals.containsKey(nodeId)) {
            Optional.ofNullable(snapshotInsertTotals.get(nodeId).get(table)).ifPresent(counter -> counter.inc(value));
        }
    }
}
