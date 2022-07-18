package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hazelcast.jet.core.Inbox;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageResult;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.ResetCounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.metrics.TaskSampleRetriever;
import io.tapdata.milestone.MilestoneContext;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:58
 **/
public abstract class HazelcastTargetPdkBaseNode extends HazelcastPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkBaseNode.class);
	protected Map<String, SyncProgress> syncProgressMap = new ConcurrentHashMap<>();
	protected Map<String, String> tableNameMap;
	protected String tableName;
	private AtomicBoolean firstBatchEvent = new AtomicBoolean();
	private AtomicBoolean firstStreamEvent = new AtomicBoolean();
	protected List<String> updateConditionFields;
	protected String writeStrategy = "updateOrInsert";
	private AtomicBoolean flushOffset = new AtomicBoolean(false);
	protected ResetCounterSampler resetInputCounter;
	protected CounterSampler inputCounter;
	protected ResetCounterSampler resetInsertedCounter;
	protected CounterSampler insertedCounter;
	protected ResetCounterSampler resetUpdatedCounter;
	protected CounterSampler updatedCounter;
	protected ResetCounterSampler resetDeletedCounter;
	protected CounterSampler deletedCounter;
	protected SpeedSampler inputQPS;
	protected AverageSampler timeCostAvg;
	protected AtomicBoolean uploadDagService;
	private Map<String, MetadataInstancesDto> uploadMetadata;

	public HazelcastTargetPdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		initMilestoneService(MilestoneContext.VertexType.DEST);
		// MILESTONE-INIT_TRANSFORMER-RUNNING
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.RUNNING);
	}

	@Override
	protected void initSampleCollector() {
		super.initSampleCollector();

		// init statistic and sample related initialize
		// TODO: init outputCounter initial value
		Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList("inputTotal", "insertedTotal", "updatedTotal", "deletedTotal"));
		resetInputCounter = statisticCollector.getResetCounterSampler("inputTotal");
		inputCounter = sampleCollector.getCounterSampler("inputTotal");
		inputCounter.inc(values.getOrDefault("inputTotal", 0).longValue());
		resetInsertedCounter = statisticCollector.getResetCounterSampler("insertedTotal");
		insertedCounter = sampleCollector.getCounterSampler("insertedTotal");
		insertedCounter.inc(values.getOrDefault("insertedTotal", 0).longValue());
		resetUpdatedCounter = statisticCollector.getResetCounterSampler("updatedTotal");
		updatedCounter = sampleCollector.getCounterSampler("updatedTotal");
		updatedCounter.inc(values.getOrDefault("updatedTotal", 0).longValue());
		resetDeletedCounter = statisticCollector.getResetCounterSampler("deletedTotal");
		deletedCounter = sampleCollector.getCounterSampler("deletedTotal");
		deletedCounter.inc(values.getOrDefault("deletedTotal", 0).longValue());
		inputQPS = sampleCollector.getSpeedSampler("inputQPS");
		timeCostAvg = sampleCollector.getAverageSampler("timeCostAvg");

		statisticCollector.addSampler("replicateLag", () -> {
			Long ts = null;
			for (SyncProgress progress : syncProgressMap.values()) {
				if (null == progress.getSourceTime()) continue;
				if (ts == null || ts > progress.getSourceTime()) {
					ts = progress.getSourceTime();
				}
			}

			return ts == null ? 0 : System.currentTimeMillis() - ts;
		});
	}

	@Override
	protected void init(@NotNull Context context) throws Exception {
		super.init(context);
		try {
			createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
			connectorNodeInit(dataProcessorContext);
		} catch (Throwable e) {
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			throw new RuntimeException(e);
		}
		this.uploadDagService = new AtomicBoolean(false);
		this.uploadMetadata = new ConcurrentHashMap<>();
	}

	@Override
	final public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			if (!inbox.isEmpty()) {
				while (isRunning()) {
					List<TapdataEvent> tapdataEvents = new ArrayList<>();
					final int count = inbox.drainTo(tapdataEvents, 1000);
					if (count > 0) {
						List<TapEvent> tapEvents = new ArrayList<>();
						List<TapdataShareLogEvent> tapdataShareLogEvents = new ArrayList<>();
						if (null != getConnectorNode()) {
							codecsFilterManager = getConnectorNode().getCodecsFilterManager();
						}
						AtomicReference<TapdataEvent> lastDmlTapdataEvent = new AtomicReference<>();
						for (TapdataEvent tapdataEvent : tapdataEvents) {
							SyncStage syncStage = tapdataEvent.getSyncStage();
							if (null != syncStage) {
								if (syncStage == SyncStage.INITIAL_SYNC && firstBatchEvent.compareAndSet(false, true)) {
									// MILESTONE-WRITE_SNAPSHOT-RUNNING
									MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_SNAPSHOT, MilestoneStatus.RUNNING);
								} else if (syncStage == SyncStage.CDC && firstStreamEvent.compareAndSet(false, true)) {
									// MILESTONE-WRITE_CDC_EVENT-FINISH
									MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_CDC_EVENT, MilestoneStatus.FINISH);
								}
							}
							if (tapdataEvent instanceof TapdataHeartbeatEvent) {
								handleTapdataHeartbeatEvent(tapdataEvent);
							} else if (tapdataEvent instanceof TapdataCompleteSnapshotEvent) {
								handleTapdataCompleteSnapshotEvent();
							} else if (tapdataEvent instanceof TapdataStartCdcEvent) {
								handleTapdataStartCdcEvent(tapdataEvent);
							} else if (tapdataEvent instanceof TapdataShareLogEvent) {
								handleTapdataShareLogEvent(tapdataShareLogEvents, tapdataEvent, lastDmlTapdataEvent::set);
							} else {
								if (tapdataEvent.isDML()) {
									handleTapdataRecordEvent(tapdataEvent, tapEvents, lastDmlTapdataEvent::set);
								} else if (tapdataEvent.isDDL()) {
									handleTapdataDDLEvent(tapdataEvent, tapEvents, lastDmlTapdataEvent::set);
								} else {
									if (null != tapdataEvent.getTapEvent()) {
										logger.warn("Tap event type does not supported: " + tapdataEvent.getTapEvent().getClass() + ", will ignore it");
									}
								}
							}
						}
						if (CollectionUtils.isNotEmpty(tapEvents)) {
							resetInputCounter.inc(tapEvents.size());
							inputCounter.inc(tapEvents.size());
							inputQPS.add(tapEvents.size());
							processEvents(tapEvents);
						}
						if (CollectionUtils.isNotEmpty(tapdataShareLogEvents)) {
							resetInputCounter.inc(tapdataShareLogEvents.size());
							inputCounter.inc(tapdataShareLogEvents.size());
							inputQPS.add(tapdataShareLogEvents.size());
							processShareLog(tapdataShareLogEvents);
						}
						flushSyncProgressMap(lastDmlTapdataEvent.get());
					} else {
						break;
					}
				}
			}
		} catch (Exception e) {
			logger.error("Target process failed {}", e.getMessage(), e);
			throw sneakyThrow(e);
		}
	}

	private void handleTapdataShareLogEvent(List<TapdataShareLogEvent> tapdataShareLogEvents, TapdataEvent tapdataEvent, Consumer<TapdataEvent> consumer) {
		tapdataShareLogEvents.add((TapdataShareLogEvent) tapdataEvent);
		if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
			consumer.accept(tapdataEvent);
		}
	}

	private void handleTapdataStartCdcEvent(TapdataEvent tapdataEvent) {
		// MILESTONE-WRITE_CDC_EVENT-RUNNING
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_CDC_EVENT, MilestoneStatus.RUNNING);
		flushSyncProgressMap(tapdataEvent);
		saveToSnapshot();
	}

	private void handleTapdataCompleteSnapshotEvent() {
		// MILESTONE-WRITE_SNAPSHOT-FINISH
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_SNAPSHOT, MilestoneStatus.FINISH);
	}

	private void handleTapdataHeartbeatEvent(TapdataEvent tapdataEvent) {
		flushSyncProgressMap(tapdataEvent);
		saveToSnapshot();
	}

	private void handleTapdataRecordEvent(TapdataEvent tapdataEvent, List<TapEvent> tapEvents, Consumer<TapdataEvent> consumer) {
		TapRecordEvent tapRecordEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
		if (writeStrategy.equals(MergeTableProperties.MergeType.appendWrite.name())) {
			if (!(tapRecordEvent instanceof TapInsertRecordEvent)) {
				return;
			}
		}
		fromTapValue(TapEventUtil.getBefore(tapRecordEvent), codecsFilterManager);
		fromTapValue(TapEventUtil.getAfter(tapRecordEvent), codecsFilterManager);
		tapEvents.add(tapRecordEvent);
		if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
			consumer.accept(tapdataEvent);
		}
	}

	private void handleTapdataDDLEvent(TapdataEvent tapdataEvent, List<TapEvent> tapEvents, Consumer<TapdataEvent> consumer) {
		TapDDLEvent tapDDLEvent = (TapDDLEvent) tapdataEvent.getTapEvent();
		updateMemoryFromDDLInfoMap(tapdataEvent, getTgtTableNameFromTapEvent(tapDDLEvent));
		Object uploadMetadata = tapDDLEvent.getInfo(UPLOAD_METADATA_INFO_KEY);
		if (uploadMetadata instanceof Map) {
			this.uploadMetadata.putAll((Map<? extends String, ? extends MetadataInstancesDto>) uploadMetadata);
		}
		tapEvents.add(tapDDLEvent);
		if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
			consumer.accept(tapdataEvent);
		}
	}

	private void flushSyncProgressMap(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) return;
		Node<?> node = processorBaseContext.getNode();
		if (CollectionUtils.isEmpty(tapdataEvent.getNodeIds())) return;
		String progressKey = tapdataEvent.getNodeIds().get(0) + "," + node.getId();
		SyncProgress syncProgress = this.syncProgressMap.get(progressKey);
		if (null == syncProgress) {
			syncProgress = new SyncProgress();
			this.syncProgressMap.put(progressKey, syncProgress);
		}
		if (tapdataEvent instanceof TapdataStartCdcEvent) {
			if (null == tapdataEvent.getSyncStage()) return;
			syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
		} else if (tapdataEvent instanceof TapdataHeartbeatEvent) {
			if (null != tapdataEvent.getStreamOffset()) {
				syncProgress.setStreamOffsetObj(tapdataEvent.getStreamOffset());
			}
			flushOffset.set(true);
		} else {
			if (null == tapdataEvent.getSyncStage()) return;
			if (null == tapdataEvent.getBatchOffset() && null == tapdataEvent.getStreamOffset()) return;
			if (SyncStage.CDC == tapdataEvent.getSyncStage() && null == tapdataEvent.getSourceTime()) return;
			if (null != tapdataEvent.getBatchOffset()) {
				syncProgress.setBatchOffsetObj(tapdataEvent.getBatchOffset());
			}
			if (null != tapdataEvent.getStreamOffset()) {
				syncProgress.setStreamOffsetObj(tapdataEvent.getStreamOffset());
			}
			if (null != tapdataEvent.getSyncStage() && null != syncProgress.getSyncStage() && !syncProgress.getSyncStage().equals(SyncStage.CDC.name())) {
				syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
			}
			syncProgress.setSourceTime(tapdataEvent.getSourceTime());
			if (tapdataEvent.getTapEvent() instanceof TapRecordEvent) {
				syncProgress.setEventTime(((TapRecordEvent) tapdataEvent.getTapEvent()).getReferenceTime());
			}
			syncProgress.setType(tapdataEvent.getType());
			flushOffset.set(true);
		}
		syncProgress.setEventSerialNo(syncProgress.addAndGetSerialNo(1));
	}

	abstract void processEvents(List<TapEvent> tapEvents);

	abstract void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents);

	protected String getTgtTableNameFromTapEvent(TapEvent tapEvent) {
		if (StringUtils.isNotBlank(tableName)) {
			return tableName;
		} else if (null != tableNameMap) {
			String tableId = TapEventUtil.getTableId(tapEvent);
			return tableNameMap.get(tableId);
		} else {
			return "";
		}
	}

	@Override
	public boolean saveToSnapshot() {
		if (!flushOffset.get()) return true;
		if (MapUtils.isEmpty(syncProgressMap)) return true;
		Map<String, String> syncProgressJsonMap = new HashMap<>(syncProgressMap.size());
		for (Map.Entry<String, SyncProgress> entry : syncProgressMap.entrySet()) {
			String key = entry.getKey();
			SyncProgress syncProgress = entry.getValue();
			List<String> list = Arrays.asList(key.split(","));
			if (null != syncProgress.getBatchOffsetObj()) {
				syncProgress.setBatchOffset(PdkUtil.encodeOffset(syncProgress.getBatchOffsetObj()));
			}
			if (null != syncProgress.getStreamOffsetObj()) {
				syncProgress.setStreamOffset(PdkUtil.encodeOffset(syncProgress.getStreamOffsetObj()));
			}
			try {
				syncProgressJsonMap.put(JSONUtil.obj2Json(list), JSONUtil.obj2Json(syncProgress));
			} catch (JsonProcessingException e) {
				throw new RuntimeException("Convert offset to json failed, errors: " + e.getMessage(), e);
			}
		}
		SubTaskDto subTaskDto = dataProcessorContext.getSubTaskDto();
		String collection = ConnectorConstant.SUB_TASK_COLLECTION + "/syncProgress/" + subTaskDto.getId();
		try {
			clientMongoOperator.insertOne(syncProgressJsonMap, collection);
		} catch (Exception e) {
			throw new RuntimeException("Save to snapshot failed, collection: " + collection + ", object: " + this.syncProgressMap + "errors: " + e.getMessage(), e);
		}
		if (uploadDagService.get()) {
			if (MapUtils.isNotEmpty(uploadMetadata)) {
				// Upload Metadata
				TransformerWsMessageResult wsMessageResult = new TransformerWsMessageResult();
				wsMessageResult.setBatchMetadataUpdateMap(uploadMetadata);
				// 返回结果调用接口返回
				clientMongoOperator.insertOne(wsMessageResult, ConnectorConstant.TASK_COLLECTION + "/transformer/resultWithHistory");
				uploadMetadata.clear();
			}
			// Upload DAG
			TaskDto taskDto = new TaskDto();
			taskDto.setId(dataProcessorContext.getSubTaskDto().getParentId());
			taskDto.setDag(dataProcessorContext.getSubTaskDto().getDag());
			clientMongoOperator.insertOne(taskDto, ConnectorConstant.TASK_COLLECTION + "/dag");
			uploadDagService.compareAndSet(true, false);
		}
		return true;
	}
}