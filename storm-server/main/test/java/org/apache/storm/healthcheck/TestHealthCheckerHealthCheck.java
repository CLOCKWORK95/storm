package org.apache.storm.healthcheck;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.HashMap;
import java.util.Map;
import java.lang.Exception;
import org.apache.storm.metric.StormMetricsRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.BeforeEach;

public class TestHealthCheckerHealthCheck {

    HealthChecker           healthChecker;


    @BeforeEach
    public void configure(){
        this.healthChecker = new HealthChecker();
    }

    @ParameterizedTest
    @MethodSource
    public void testHealthCheck(String key, Object value, StormMetricsRegistry metricRegistry, Object expectedResult){
        Map<String,Object> conf = new HashMap();
        
        try{
            conf.put(key, value);
            int result = healthChecker.healthCheck(conf, metricRegistry);
            assertEquals(expectedResult,result);
        } catch(Exception e){
            assertEquals(expectedResult,e.getClass());
        }
    }

    private static Stream<Arguments> testHealthCheck(){
        //  String       Object       metricRegistry
        return Stream.of(       
                Arguments.of("key",            new DummyObject(),       new StormMetricsRegistry(),     0),
                Arguments.of("key",            null,                    new StormMetricsRegistry(),     0),
                Arguments.of("key",            new DummyObject(),       null,                           0),
                Arguments.of(null,             new DummyObject(),       new StormMetricsRegistry(),     0)
        );
    }


    public static class DummyObject{
        private String foo;
        public DummyObject(){
            this.foo = "foo";
        }
    }

}
