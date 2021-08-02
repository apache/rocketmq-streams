package org.apache.rocketmq.streams.serviceloader;

import org.apache.rocketmq.streams.serviceloader.namefinder.IServiceNameGetter;
import org.apache.rocketmq.streams.serviceloader.namefinder.impl.AnnotationServiceNameGetter;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class ServiceLoaderComponentTest {

    @Test
    public void testLoadService() {
        ServiceLoaderComponent serviceLoaderComponent = ServiceLoaderComponent.getInstance(IServiceNameGetter.class);
        AnnotationServiceNameGetter getter = (AnnotationServiceNameGetter)serviceLoaderComponent.getService().loadService(AnnotationServiceNameGetter.SERVICE_NAME);
        assertTrue(getter != null && AnnotationServiceNameGetter.class.isInstance(getter));
    }
}
