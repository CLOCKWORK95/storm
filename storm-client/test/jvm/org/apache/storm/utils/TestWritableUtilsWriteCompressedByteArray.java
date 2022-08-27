package org.apache.storm.utils;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestWritableUtilsWriteCompressedByteArray {

    private static DataOutput dos;
    private static ByteArrayOutputStream baos;

    private static Stream<Arguments> testWriteCompressedByteArray() throws IOException{
        //      DataInput       ExpectedResult
        return Stream.of(       
                Arguments.of( STREAM_CASES.VALID,       "foo byte array!".getBytes(),           ( 100 * getCompressedSize("foo byte array!".getBytes()) )/"foo byte array!".getBytes().length),
                Arguments.of( STREAM_CASES.VALID,       "".getBytes(),                          0),
                Arguments.of( STREAM_CASES.VALID,       null,                                   -1),
                Arguments.of( STREAM_CASES.INVALID,     "foo byte array!".getBytes(),           ClassCastException.class),
                Arguments.of( STREAM_CASES.NULL,        "foo byte array!".getBytes(),           NullPointerException.class)
        );
    }

    public static int getCompressedSize(byte[] content) throws IOException{
        byte[] compressed =     null;
        ByteArrayOutputStream   baos;
        ByteArrayOutputStream   baos2;
        GZIPOutputStream        gzos;
        DataOutputStream        dos;
        try{
            baos = new ByteArrayOutputStream();
            gzos = new GZIPOutputStream(baos);
            baos2 = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos2);

            gzos.write(content);
            gzos.close();
            compressed = baos.toByteArray();
            baos.close();

            //dos.writeInt(compressed.length);
            dos.write(compressed);
            dos.close();
            compressed = baos2.toByteArray();
            baos2.close();
        } catch( Exception e){
            e.printStackTrace();
        }
        return compressed.length;
    }


    @BeforeEach
    public void configure() throws IOException{
        baos = new ByteArrayOutputStream();
        dos = new DataOutputStream(baos);
    }


    @ParameterizedTest
    @MethodSource
    public void testWriteCompressedByteArray( STREAM_CASES dosCase, byte[] content, Object expectedResult){
        int res;
        try{
            switch( dosCase ){
                case VALID:
                    res = WritableUtils.writeCompressedByteArray(dos, content);
                    assertEquals((int) expectedResult, res);
                    break;
                case INVALID:
                    baos.close();
                    res = WritableUtils.writeCompressedByteArray(dos, content);
                    assertEquals((int) expectedResult, res);
                    break;
                case NULL:
                    baos = null;
                    dos = null;
                    res = WritableUtils.writeCompressedByteArray(dos, content);
                    assertEquals((int) expectedResult, res);
                    break;
                default:
                    break;
            }
        }
        catch (Exception e){
            assertEquals(expectedResult, e.getClass());
        }
    }


    @AfterEach
    public void tearDown(){
        try{
            baos.close();
        } catch( Exception e ){
            e.printStackTrace();
        }
    }



    public enum STREAM_CASES{
        VALID,
        INVALID,
        NULL
    }

}
