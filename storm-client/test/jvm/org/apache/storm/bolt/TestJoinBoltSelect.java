package org.apache.storm.bolt;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.TupleWindowImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestJoinBoltSelect {

    static String[] streamFields = {"field1", "field2"};
    static Object[][] streamData = {
        {1,             "data1"},
        {2,             "data2"},
        {3,             null},
        {null,           "data4"}
    };
    
    private static CustomCollector  mockedCollector;
    private static ArrayList<Tuple> inputStream;
    private static TupleWindow      window;
    private static JoinBolt         bolt;
    


    private static ArrayList<Tuple> createNewStream(String streamName, String[] fieldNames, Object[][] data, String srcComponentName) {
        GeneralTopologyContext mockContext = new CustomContext(fieldNames);
        ArrayList<Tuple> stream = new ArrayList<>();
        for (Object[] entry : data) {
            TupleImpl rec = new TupleImpl(mockContext, Arrays.asList(entry), srcComponentName, 0, streamName);
            stream.add(rec);
        }
        return stream;
    }


    private static TupleWindow createNewWindowForSingleStream(ArrayList<Tuple> stream) {
        return new TupleWindowImpl(stream, null, null); 
    }

    /* 
    private static TupleWindow createNewWindowForMultipleStreams(ArrayList<Tuple> ... streams) {
        if (streams == null) return new TupleWindowImpl(new ArrayList<Tuple>(), null, null); 
        ArrayList<Tuple> allStreams= new ArrayList<>();
        for (int i = 0; i < streams.length; i++) {
            allStreams.addAll(streams[i]);
        }
        return new TupleWindowImpl(allStreams, null, null);
    }
     */
    
     @BeforeEach
    public void configure(){
        mockedCollector = new CustomCollector();
        inputStream = createNewStream("streamName", streamFields, streamData, "streamSpout");
        window = createNewWindowForSingleStream(inputStream);
        bolt = new JoinBolt(JoinBolt.Selector.STREAM, "streamName", streamFields[0]);
    }

    @ParameterizedTest
    @MethodSource
    public void testJoinBoltSelect(String commaSeparatedValues, Object expectedResult){
        try{
            bolt.select(commaSeparatedValues);
            bolt.prepare(null, null, mockedCollector);
            bolt.execute(window);
            assertEquals(expectedResult, mockedCollector.outputs.size());
        } catch(Exception e){
            assertEquals(expectedResult, e.getClass());
        }
    }

    
    private static Stream<Arguments> testJoinBoltSelect(){
        //  streamName       expectedResult (result size)
        return Stream.of(       
                Arguments.of( "tableNumber,name",   4),
                Arguments.of( "",                   4),
                Arguments.of( "tableNumber",        4)
        );
    }


}
