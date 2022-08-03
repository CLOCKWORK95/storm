package org.apache.storm.healthcheck;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.lang.Exception;
import org.apache.storm.DaemonConfig;
import org.apache.storm.Config;
import org.apache.storm.metric.StormMetricsRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.BeforeEach;

public class TestHealthCheckerProcessScript {

    HealthChecker           healthChecker;


    @BeforeEach
    public void configure(){
        this.healthChecker = new HealthChecker();
    }

    @ParameterizedTest
    @MethodSource
    public void testProcessScript(String key, Object value, StormMetricsRegistry metricRegistry, Object expectedResult){
        Map<String, Object> conf = new HashMap(); 
        File f = new File((String) value);
        f.mkdir();
        try{
            conf.put(key, f);
            int result = healthChecker.healthCheck(conf, metricRegistry);
            assertEquals(expectedResult,result);
        } catch(Exception e){
            assertEquals(expectedResult,e.getClass());
        }
    }

    private static Stream<Arguments> testProcessScript(){
        //  String       Object       metricRegistry
        return Stream.of(       
                //Arguments.of("key",                         new DummyObject(),       new StormMetricsRegistry(),     0),
                Arguments.of(DaemonConfig.STORM_HEALTH_CHECK_DIR,        "../dir",                    new StormMetricsRegistry(),    ClassCastException.class)
                //Arguments.of("key",                         new DummyObject(),       null,                           0),
                //Arguments.of(null,                          new DummyObject(),       new StormMetricsRegistry(),     0)
        );
    }


    public static class DummyObject{
        private String foo;
        public DummyObject(){
            this.foo = "foo";
        }
    }

}
