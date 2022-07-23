package org.apache.storm.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.TupleWindowImpl;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestJoinBoltSelect {

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
    


    private static ArrayList<Tuple> makeStream(String streamName, String[] fieldNames, Object[][] data, String srcComponentName) {
        ArrayList<Tuple> result = new ArrayList<>();
        MockContext mockContext = new MockContext(fieldNames);

        for (Object[] record : data) {
            TupleImpl rec = new TupleImpl(mockContext, Arrays.asList(record), srcComponentName, 0, streamName);
            result.add(rec);
        }

        return result;
    }

    private static TupleWindow makeTupleWindow(ArrayList<Tuple>... streams) {
        ArrayList<Tuple> combined = null;
        for (int i = 0; i < streams.length; i++) {
            if (i == 0) {
                combined = new ArrayList<>(streams[0]);
            } else {
                combined.addAll(streams[i]);
            }
        }
        return new TupleWindowImpl(combined, null, null);
    }

    @ParameterizedTest
    @MethodSource
    public void testJoinBoltSelect( String srcOrStreamId, String fieldName, Object expectedResult ){

        ArrayList<Tuple> orderStream = makeStream("orders", reservationsFields, reservations, "ordersSpout");
        TupleWindow window = makeTupleWindow(orderStream);

        JoinBolt bolt = new JoinBolt(JoinBolt.Selector.STREAM, "orders", reservationsFields[0])
            .select("tableNumber,name");
        MockCollector collector = new MockCollector();
        bolt.prepare(null, null, collector);
        bolt.execute(window);
        assertEquals(orderStream.size(), collector.actualResults.size());

    }

    
    private static Stream<Arguments> testJoinBoltSelect(){
        //  srcOrStreamId       fieldName       expectedValue
        return Stream.of(       
                Arguments.of("",            "",             JoinBolt.class),
                Arguments.of(null,          null,           NullPointerException.class),
                Arguments.of("sourceId",    "fieldName",    JoinBolt.class)
        );
    }



    static class MockCollector extends OutputCollector {
        public ArrayList<List<Object>> actualResults = new ArrayList<>();

        public MockCollector() {
            super(null);
        }

        @Override
        public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple) {
            actualResults.add(tuple);
            return null;
        }

    } // class MockCollector

    static class MockContext extends GeneralTopologyContext {

        private final Fields fields;

        public MockContext(String[] fieldNames) {
            super(null, new HashMap<>(), null, null, null, null);
            this.fields = new Fields(fieldNames);
        }

        @Override
        public String getComponentId(int taskId) {
            return "component";
        }

        @Override
        public Fields getComponentOutputFields(String componentId, String streamId) {
            return fields;
        }

    }

}
