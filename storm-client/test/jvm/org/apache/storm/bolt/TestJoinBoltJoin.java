package org.apache.storm.bolt;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.apache.storm.bolt.TestJoinBoltJoin.StreamGenerator.TupleStream;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.TupleWindowImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestJoinBoltJoin {

    static String[] reservationsFields = {"tableNumber", "name"};
    static Object[][] reservations = {
        {1,             "Rossi"},
        {2,             "Neri"},
        {3,             "Verdi"},
        {4,             "Bianchi"},
        {5,             "Monti"},
        {6,             null}
    };

    static String[] menuFields = {"entry", "price"};
    static Object[][] menu = {
        {"sandwich",    6.00},
        {"coke",        1.50},
        {"beer",        4.00},
        {"salad",       7.00},
        {"water",       1.50},
        {"fries",       null}
    };

    static String[] ordersFields = {"reservation", "order"};
    static Object[][] orders = {
        {1,             "sandwich"},
        {2,             "coke"},
        {1,             "beer"},
        {3,             "salad"},
        {3,             "salad"}
    };

    private static CustomCollector  mockedCollector;


    private static ArrayList<Tuple> createNewStream(String streamName, String[] fieldNames, Object[][] data, String srcComponentName) {
        GeneralTopologyContext mockContext = new CustomContext(fieldNames);
        ArrayList<Tuple> stream = new ArrayList<>();
        for (Object[] entry : data) {
            TupleImpl rec = new TupleImpl(mockContext, Arrays.asList(entry), srcComponentName, 0, streamName);
            stream.add(rec);
        }
        return stream;
    }

    @SafeVarargs
    private final TupleWindow createNewWindow(ArrayList<Tuple> ... streams) {
        if (streams == null) return new TupleWindowImpl(new ArrayList<Tuple>(), null, null); 
        ArrayList<Tuple> allStreams= new ArrayList<>();
        for (int i = 0; i < streams.length; i++) {
            allStreams.addAll(streams[i]);
        }
        return new TupleWindowImpl(allStreams, null, null);
    }


    private static TupleStream  stream1;
    private static TupleStream  stream2;
    private static JoinBolt     bolt;


    @BeforeEach
    public void configure(){
        mockedCollector = new CustomCollector();
        stream1 = StreamGenerator.createStream(STREAM.MENU, 0);
        bolt = new JoinBolt(JoinBolt.Selector.STREAM, stream1.streamName, stream1.streamFields[stream1.fieldIndex]);
    }


    @ParameterizedTest
    @MethodSource
    public void testJoinBoltJoin( STREAM secondStream, int field2Index, STREAM thirdStream, int field3Index, JOINTYPE jointype, Object expectedResult ){

        stream2 = StreamGenerator.createStream(secondStream, field2Index);

        switch(jointype){

            case LEFT:
                try{
                   
                    TupleWindow window = createNewWindow(stream1.inputStream, stream2.inputStream);

                    bolt.leftJoin(stream2.streamName, stream2.streamFields[stream2.fieldIndex], stream1.streamName);
                    bolt.select(stream1.commaSeparatedValues + stream2.commaSeparatedValues);
                    bolt.prepare(null, null, mockedCollector);
                    bolt.execute(window);
                    
                    assertEquals(expectedResult, mockedCollector.outputs.size());
                } catch(Exception e){
                    assertEquals(expectedResult, e.getClass());
                }
                break;

            case INNER:
                try{
                    
                    TupleWindow window = createNewWindow(stream1.inputStream, stream2.inputStream);

                    bolt.join(stream2.streamName, stream2.streamFields[stream2.fieldIndex], stream1.streamName);
                    bolt.select(stream1.commaSeparatedValues + stream2.commaSeparatedValues);
                    bolt.prepare(null, null, mockedCollector);
                    bolt.execute(window);
                    
                    assertEquals(expectedResult, mockedCollector.outputs.size());
                } catch(Exception e){
                    assertEquals(expectedResult, e.getClass());
                }
                break;
            
            case EMPTY_STRING_JOIN:
                try{
                    
                    TupleWindow window = createNewWindow(stream1.inputStream, stream2.inputStream);

                    bolt.join("", "", stream1.streamName);
                    bolt.select(stream1.commaSeparatedValues);
                    bolt.prepare(null, null, mockedCollector);
                    bolt.execute(window);
                    
                    assertEquals(expectedResult, mockedCollector.outputs.size());
                } catch(Exception e){
                    assertEquals(expectedResult, e.getClass());
                }
                break;

            case SAME_STREAM_JOIN:
                try{
                    
                    TupleWindow window = createNewWindow(stream1.inputStream, stream2.inputStream);

                    bolt.join(stream2.streamName, stream2.streamFields[stream2.fieldIndex], stream1.streamName);
                    bolt.join(stream2.streamName, stream2.streamFields[stream2.fieldIndex], stream1.streamName);

                    bolt.select(stream1.commaSeparatedValues);
                    bolt.prepare(null, null, mockedCollector);
                    bolt.execute(window);
                    
                    assertEquals(expectedResult, mockedCollector.outputs.size());
                } catch(Exception e){
                    assertEquals(expectedResult, e.getClass());
                }
                break;
            
            case NOT_EXISTING_PRIOR:
                try{
                    
                    TupleWindow window = createNewWindow(stream1.inputStream, stream2.inputStream);

                    bolt.join(stream2.streamName, stream2.streamFields[stream2.fieldIndex], "foo");

                    bolt.select(stream1.commaSeparatedValues);
                    bolt.prepare(null, null, mockedCollector);
                    bolt.execute(window);
                    
                    assertEquals(expectedResult, mockedCollector.outputs.size());
                } catch(Exception e){
                    assertEquals(expectedResult, e.getClass());
                }
                break;
            

            default:
                break;
        }
    }


    
    private static Stream<Arguments> testJoinBoltJoin(){
        //                 newStreamName    newKeyField     newFurtherStreamName   newFurtherkeyField   joinType        expectedResult
        return Stream.of(   
                // Test Suite Minimale    
                Arguments.of( STREAM.RESERVATIONS,      1,        null,      0,        JOINTYPE.LEFT,                  6),
                Arguments.of( STREAM.ORDERS,            1,        null,      0,        JOINTYPE.INNER,                 5),
                Arguments.of( STREAM.ORDERS,            1,        null,      0,        JOINTYPE.EMPTY_STRING_JOIN,     RuntimeException.class),
                Arguments.of( STREAM.ORDERS,            1,        null,      0,        JOINTYPE.NOT_EXISTING_PRIOR,    IllegalArgumentException.class),
                Arguments.of( STREAM.NULL,              1,        null,      0,        JOINTYPE.LEFT,                  NullPointerException.class),

                // Control Flow Coverage
                Arguments.of( STREAM.ORDERS,            1,        null,      0,        JOINTYPE.SAME_STREAM_JOIN,      IllegalArgumentException.class)
        );
    }


    public enum JOINTYPE{
        LEFT, 
        INNER,
        EMPTY_STRING_JOIN,
        SAME_STREAM_JOIN,
        NOT_EXISTING_PRIOR,
        TRIPLE_LEFT,
        TRIPLE_INNER,
        TRIPLE_LEFT_INNER
    }

    public enum STREAM{
        RESERVATIONS,
        MENU,
        ORDERS,
        NULL
    }


    public static class StreamGenerator{

        public static TupleStream createStream( STREAM stream, int fieldIndex ){
            switch(stream){
                case RESERVATIONS:
                    return new ReservationStream(fieldIndex);
                case MENU:
                    return new MenuStream(fieldIndex);
                case ORDERS:
                    return new OrderStream(fieldIndex);
                case NULL:
                    return new TupleStreamImpl();
                default:
                    return new TupleStreamImpl();
            }
        }

        public static abstract class TupleStream{
            public ArrayList<Tuple>     inputStream     = null;
            public String[]             streamFields    = null;
            public Object[][]           streamData      = null;
            public String               commaSeparatedValues = null;
            public String               streamName      = null;
            public int                  fieldIndex      = 0;
            
            public TupleStream(){}

            public TupleStream( String[] streamFields, Object[][] streamData ){
                this.streamFields = streamFields;
                this.streamData = streamData;
                for ( int i = 0; i < streamFields.length; i ++){
                    this.commaSeparatedValues += streamFields[i];
                }
            }
        }

        public static class TupleStreamImpl extends TupleStream{
            public TupleStreamImpl(){
                super();
            }
        }

        public static class ReservationStream extends TupleStream{
            public ReservationStream( int field1Index ){
                super( reservationsFields, reservations );
                this.fieldIndex = field1Index;
                this.streamName = "reservations";
                this.inputStream = createNewStream("reservations", this.streamFields, this.streamData, "reservationsSpout");
            }
        }
        public static class MenuStream extends TupleStream{
            public MenuStream( int field1Index ){
                super( menuFields, menu );
                this.fieldIndex = field1Index;
                this.streamName = "menu";
                this.inputStream = createNewStream("menu", this.streamFields, this.streamData, "menuSpout");
            }
        }
        public static class OrderStream extends TupleStream{
            public OrderStream( int field1Index ){
                super( ordersFields, orders );
                this.fieldIndex = field1Index;
                this.streamName = "orders";
                this.inputStream = createNewStream("orders", this.streamFields, this.streamData, "ordersSpout");
            }
        }
    }

    
}
