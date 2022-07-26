package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.LRUMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.voovan.tools.collection.CacheMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HazelcastSampleSourcePdkDataNode extends HazelcastSourcePdkDataNode {

  private final Logger logger = LogManager.getLogger(HazelcastSampleSourcePdkDataNode.class);

  private static final String TAG = HazelcastSampleSourcePdkDataNode.class.getSimpleName();

  private static final CacheMap<String, List<TapEvent>> sampleDataCacheMap = new CacheMap<>();


  static {
    sampleDataCacheMap.maxSize(100).autoRemove(true).expire(600).interval(60).create();
  }
  public HazelcastSampleSourcePdkDataNode(DataProcessorContext dataProcessorContext) {
    super(dataProcessorContext);
  }

  @Override
  public void startSourceRunner() {

    try {
      Node<?> node = dataProcessorContext.getNode();
      Thread.currentThread().setName("PDK-SAMPLE-SOURCE-RUNNER-" + node.getName() + "(" + node.getId() + ")");
      Log4jUtil.setThreadContext(dataProcessorContext.getSubTaskDto());
      TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
      // 测试任务
      long startTs = System.currentTimeMillis();
      for (String tableName : tapTableMap.keySet()) {
        if (!isRunning()) {
          break;
        }
        TapTable tapTable = tapTableMap.get(tableName);
        String sampleDataId = ((TableNode) node).getConnectionId() + "_" + tableName;

        List<TapEvent> tapEventList = sampleDataCacheMap.getOrDefault(sampleDataId, new ArrayList<>());
        boolean isCache = true;
        if (CollectionUtils.isEmpty(tapEventList)) {
          isCache = false;
          QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = getConnectorNode().getConnectorFunctions().getQueryByAdvanceFilterFunction();
          TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().limit(1);
          PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
                  () -> queryByAdvanceFilterFunction.query(getConnectorNode().getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {

                    List<Map<String, Object>> results = filterResults.getResults();
                    List<TapEvent> events = wrapTapEvent(results, tapTable.getId());
                    if (CollectionUtil.isNotEmpty(events)) {
                      tapEventList.addAll(events);
                    }
                  }), TAG);
          sampleDataCacheMap.put(sampleDataId, tapEventList);
        }

        if (logger.isDebugEnabled()) {
          logger.debug("get sample data, cache [{}], cost {}ms", isCache, (System.currentTimeMillis() - startTs));
        }

        List<TapEvent> cloneList = new ArrayList<>();
        for (TapEvent tapEvent : tapEventList) {
          cloneList.add((TapEvent) tapEvent.clone());
        }

        List<TapdataEvent> tapdataEvents = wrapTapdataEvent(cloneList);
        if (CollectionUtil.isNotEmpty(tapdataEvents)) {
          tapdataEvents.forEach(this::enqueue);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.info("query sample data complet, cost {}ms", (System.currentTimeMillis() - startTs));
      }

    } catch (Throwable throwable) {
      error = throwable;
    } finally {
      this.running.set(false);
    }
  }

  private static List<TapEvent> wrapTapEvent(List<Map<String, Object>> results, String table) {
    List<TapEvent> tapEvents = new ArrayList<>();

    for (Map<String, Object> result : results) {
      tapEvents.add(new TapInsertRecordEvent().init().after(result).table(table));
    }

    return tapEvents;
  }
}