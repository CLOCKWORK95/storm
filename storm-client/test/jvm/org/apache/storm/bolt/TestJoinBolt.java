package org.apache.storm.bolt;

import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestJoinBolt {

    String[] reservationsFields = {"tableNumber", "name"};
    Object[][] reservations = {
        {1,             "Rossi"},
        {2,             "Neri"},
        {3,             "Verdi"},
        {4,             "Bianchi"},
        {5,             "Monti"},
        {6,             null}
    };

    String[] menuFields = {"entry", "price"};
    Object[][] menu = {
        {"sandwich",    6.00},
        {"coke",        1.50},
        {"beer",        4.00},
        {"salad",       7.00},
        {"water",       1.50},
        {"fries",       null}
    };

    String[] ordersFields = {"tableNumber", "order"};
    Object[][] orders = {
        {1,             "sandwich"},
        {2,             "coke"},
        {1,             "beer"},
        {3,             "salad"},
        {3,             "water"},
        {5,             "beer"},
        {4,             "water"},
        {6,             "salad"}
    };
    

    // Test Parameters
    private String srcOrStreamId;
    private String fieldName;
    private Object expectedResult;
    

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


    /*public TestJoinBolt(String srcOrStreamId, String fieldName, Object expectedResult){
        this.srcOrStreamId = srcOrStreamId;
        this.fieldName = fieldName;
        this.expectedResult = expectedResult;
    }*/


    /*@BeforeEach
    public void configure(){
        
    }*/




}
