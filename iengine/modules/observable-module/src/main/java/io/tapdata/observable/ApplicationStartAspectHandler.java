package io.tapdata.observable;

import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.aspect.ApplicationStartAspect;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;

/**
 * @author Dexter
 */
@AspectObserverClass(ApplicationStartAspect.class)
public class ApplicationStartAspectHandler implements AspectObserver<ApplicationStartAspect> {
    @Override
    public void observe(ApplicationStartAspect aspect) {
        ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
        CollectorFactory.getInstance().start(
                new TaskSampleReporterV2(clientMongoOperator),
                null
        );

    }
}
