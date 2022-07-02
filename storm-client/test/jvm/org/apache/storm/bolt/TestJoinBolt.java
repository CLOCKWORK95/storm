package org.apache.storm.bolt;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
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
    

    @Parameterized.Parameters
    public static Collection<Object[]> parameters(){
        return Arrays.asList(new Object[][]{
            //  srcOrStreamId       fieldName       expectedValue
            {"",            "",             Exception.class},
            {null,          null,           NullPointerException.class},
            {"sourceId",    "fieldName",    JoinBolt.class}
        });
    }


    public TestJoinBolt(String srcOrStreamId, String fieldName, Object expectedResult){
        this.srcOrStreamId = srcOrStreamId;
        this.fieldName = fieldName;
        this.expectedResult = expectedResult;
    }


    @Before
    public void configure(){
        
    }


    @Test
    public void testJoinBolt(){
        try{
            JoinBolt joiner = new JoinBolt(JoinBolt.Selector.STREAM, srcOrStreamId, fieldName);
            Assert.assertEquals(expectedResult, joiner.getClass());
        }
        catch (Exception e){
            Assert.assertEquals(expectedResult, e.getClass());
        }
    }



}
