package org.apache.storm.utils;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
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

public class TestWritableUtilsWriteCompressedStringArray {
    

    private static DataOutput               dos;
    private static ByteArrayOutputStream    baos;
    private static byte[]                   compressedStringArray = null;

    
    @BeforeEach
    public void configure() throws IOException{
        baos = new ByteArrayOutputStream();
        dos = new DataOutputStream(baos);
    }


    @ParameterizedTest
    @MethodSource
    public void testWriteCompressedByteArray( STREAM_CASES dosCase, String[] content, Object expectedResult){
        String[] reverted = null;
        try{
            switch( dosCase ){
                case VALID_INPUT_STREAM:
                    WritableUtils.writeCompressedStringArray(dos, content);
                    compressedStringArray = baos.toByteArray();
                    reverted = WritableUtils.readCompressedStringArray(new DataInputStream(new ByteArrayInputStream(compressedStringArray)));
                    assertArrayEquals( (String[]) expectedResult, reverted);
                    break;
                case INVALID_INPUT_STREAM:
                    baos.close();
                    WritableUtils.writeCompressedStringArray(dos, content);
                    compressedStringArray = baos.toByteArray();
                    reverted = WritableUtils.readCompressedStringArray(new DataInputStream(new ByteArrayInputStream(compressedStringArray)));
                    assertArrayEquals( (String[]) expectedResult, reverted);
                    break;
                case NULL:
                    baos =  null;
                    dos =   null;
                    WritableUtils.writeCompressedStringArray(dos, content);
                    assertArrayEquals( (String[]) expectedResult, reverted);
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

    
    private static Stream<Arguments> testWriteCompressedByteArray() throws IOException{
        //      DataInput       ExpectedResult
        return Stream.of(       
                Arguments.of( STREAM_CASES.VALID_INPUT_STREAM,       new String[]{"foo"," string", " array"},   new String[]{"foo"," string", " array"} ),
                Arguments.of( STREAM_CASES.VALID_INPUT_STREAM,       new String[]{},                            new String[]{}),
                Arguments.of( STREAM_CASES.NULL,                     null,                                      NullPointerException.class),
                Arguments.of( STREAM_CASES.INVALID_INPUT_STREAM,     new String[]{},                            ClassCastException.class)
        );
    }


    public enum STREAM_CASES{
        VALID_INPUT_STREAM,
        INVALID_INPUT_STREAM,
        NULL
    }

}
