package io.tapdata.observable.handler;

import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;

import java.util.*;

/**
 * @author Dexter
 */
public class TaskSampleHandler extends AbstractHandler {
    public TaskSampleHandler(SubTaskDto task) {
        super(task);
    }

    public Map<String, String> taskTags() {
        return baseTags(SAMPLE_TYPE_TASK);
    }

    private final Set<String> taskTables = new HashSet<>();
    public void addTable(String... tables) {
        taskTables.addAll(Arrays.asList(tables));
    }

    private SampleCollector collector;
    private CounterSampler createTableTotal;
    private CounterSampler snapshotTableTotal;
    private CounterSampler inputInsertTotal;
    private CounterSampler inputUpdateTotal;
    private CounterSampler inputDeleteTotal;
    private CounterSampler inputOthersTotal;
    private CounterSampler inputDdlTotal;
    private SpeedSampler inputQps;

    private CounterSampler outputInsertTotal;
    private CounterSampler outputUpdateTotal;
    private CounterSampler outputDeleteTotal;
    private CounterSampler outputOthersTotal;
    private CounterSampler outputDdlTotal;
    private SpeedSampler outputQps;

    private AverageSampler timeCostAvg;

    public void init() {
        Map<String, String> tags = taskTags();
        collector = CollectorFactory.getInstance().getSampleCollectorByTags("taskSamplers", tags);

        collector.addSampler("tableTotal", taskTables::size);
        createTableTotal = collector.getCounterSampler("createTableTotal");
        snapshotTableTotal = collector.getCounterSampler("snapshotTableTotal");

        inputInsertTotal = collector.getCounterSampler("inputInsertTotal");
        inputUpdateTotal = collector.getCounterSampler("inputUpdateTotal");
        inputDeleteTotal = collector.getCounterSampler("inputDeleteTotal");
        inputOthersTotal = collector.getCounterSampler("inputOthersTotal");
        inputDdlTotal = collector.getCounterSampler("inputDdlTotal");
        inputQps = collector.getSpeedSampler("inputQps");


        outputInsertTotal = collector.getCounterSampler("outputInsertTotal");
        outputUpdateTotal = collector.getCounterSampler("outputUpdateTotal");
        outputDeleteTotal = collector.getCounterSampler("outputDeleteTotal");
        outputOthersTotal = collector.getCounterSampler("outputOthersTotal");
        outputDdlTotal = collector.getCounterSampler("outputDdlTotal");
        outputQps = collector.getSpeedSampler("outputQps");

        timeCostAvg = collector.getAverageSampler("timeCostAvg");


    }

    public void close() {
        Optional.ofNullable(collector).ifPresent(collector -> {
            CollectorFactory.getInstance().removeSampleCollectorByTags(collector.tags());
        });
    }

    public void handleCreateTableEnd() {
        createTableTotal.inc();
    }

    public void handleBatchReadFuncEnd() {
        snapshotTableTotal.inc();
    }
}
