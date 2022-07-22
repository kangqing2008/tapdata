package io.tapdata.observable.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Dexter
 */
public class ProcessorNodeSampleHandler extends AbstractNodeSampleHandler {
    public ProcessorNodeSampleHandler(SubTaskDto task) {
        super(task);
    }


    private final Map<String, SampleCollector> collectors = new HashMap<>();

    private final Map<String, CounterSampler> inputCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputCounters = new HashMap<>();


    private final Map<String, SpeedSampler> inputSpeeds = new HashMap<>();
    private final Map<String, SpeedSampler> outputSpeeds = new HashMap<>();

    private final Map<String, AverageSampler> timeCostAverages = new HashMap<>();


    public void init(Node<?> node) {
        Map<String, String> tags = nodeTags(node);

        // TODO(dexter): use the initial value from db
        String nodeId = node.getId();
        SampleCollector collector = CollectorFactory.getInstance().getSampleCollectorByTags("nodeSamplers", tags);
        collectors.put(nodeId, collector);

        inputCounters.put(nodeId, collector.getCounterSampler("inputTotal"));
        inputSpeeds.put(nodeId, collector.getSpeedSampler("inputQPS"));
        outputCounters.put(nodeId, collector.getCounterSampler("outputTotal"));
        outputSpeeds.put(nodeId, collector.getSpeedSampler("outputQPS"));
        timeCostAverages.put(nodeId, collector.getAverageSampler("timeCostAvg"));
    }

    public void close(Node<?> node) {
        String nodeId = node.getId();
        Optional.ofNullable(collectors.get(nodeId)).ifPresent(collector -> {
            CollectorFactory.getInstance().removeSampleCollectorByTags(collector.tags());
        });
    }

    public void handleProcessStart(String nodeId, Long startAt) {
        Optional.ofNullable(inputCounters.get(nodeId)).ifPresent(CounterSampler::inc);
        Optional.ofNullable(inputSpeeds.get(nodeId)).ifPresent(SpeedSampler::add);
    }

    public void handleProcessEnd(String nodeId, Long startAt, Long endAt, long total) {
        Optional.ofNullable(outputCounters.get(nodeId)).ifPresent(counter -> counter.inc(total));
        Optional.ofNullable(outputSpeeds.get(nodeId)).ifPresent(speed -> speed.add(total));
        Optional.ofNullable(timeCostAverages.get(nodeId)).ifPresent(average ->
                average.add(total, endAt - startAt));
    }
}
