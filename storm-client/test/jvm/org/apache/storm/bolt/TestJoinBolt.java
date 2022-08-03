package org.apache.storm.bolt;

import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestJoinBolt {
    

    @ParameterizedTest
    @MethodSource
    public void testJoinBolt( String srcOrStreamId, String fieldName, Object expectedResult ){
        try{
            JoinBolt joiner = new JoinBolt(JoinBolt.Selector.STREAM, srcOrStreamId, fieldName);
            assertEquals(expectedResult, joiner.getClass());
        }
        catch (Exception e){
            assertEquals(expectedResult, e.getClass());
        }
    }

    
    private static Stream<Arguments> testJoinBolt(){
        //  srcOrStreamId       fieldName       expectedValue
        return Stream.of(       
                Arguments.of("",            "",             JoinBolt.class),
                Arguments.of(null,          null,           NullPointerException.class),
                Arguments.of("sourceId",    "fieldName",    JoinBolt.class)
        );
    }



}
