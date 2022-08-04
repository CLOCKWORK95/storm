package org.apache.storm.bolt;

import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.storm.bolt.JoinBolt.Selector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestJoinBolt {
    

    @ParameterizedTest
    @MethodSource
    public void testJoinBolt( Selector type, String srcOrStreamId, String fieldName, Object expectedResult ){
        try{
            JoinBolt joiner = new JoinBolt(type, srcOrStreamId, fieldName);
            assertEquals(expectedResult, joiner.getClass());
        }
        catch (Exception e){
            assertEquals(expectedResult, e.getClass());
        }
    }

    
    private static Stream<Arguments> testJoinBolt(){
        //  srcOrStreamId       fieldName       expectedValue
        return Stream.of(       
                Arguments.of(JoinBolt.Selector.STREAM,      "",                     "",                     JoinBolt.class),
                Arguments.of(null,             "streamName",          "keyField",              JoinBolt.class),
                Arguments.of(JoinBolt.Selector.STREAM,      null,                   null,                   NullPointerException.class),
                Arguments.of(JoinBolt.Selector.SOURCE,      "streamName",           "keyField",             JoinBolt.class),
                Arguments.of(JoinBolt.Selector.STREAM,      "streamName",           "keyField",             JoinBolt.class)
        );
    }



}
