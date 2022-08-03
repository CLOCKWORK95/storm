package org.apache.storm.utils;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestWritableUtilsReadCompressedStringArray {
    

    public static byte[] createCompressedStringArray() throws IOException{

        byte[][] content =   { "foo".getBytes(), " ".getBytes(), "string".getBytes(), " ".getBytes(), "array".getBytes() };
        ByteArrayOutputStream   baos;
        ByteArrayOutputStream   baos2;
        GZIPOutputStream        gzos;
        DataOutputStream        dos;
        try{
            byte[][] compressed = new byte[content.length][64];
            baos2 = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos2);

            dos.writeInt(content.length);

            for( int i = 0; i < content.length; i ++){

                baos = new ByteArrayOutputStream();
                gzos = new GZIPOutputStream(baos);

                gzos.write(content[i]);
                gzos.close();
                compressed[i] = baos.toByteArray();
                baos.close();

                dos.writeInt(compressed[i].length);
                dos.write(compressed[i]);
            }

            byte[] result = baos2.toByteArray();
            baos2.close();

            return result;
            
        } catch( Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] createEmptyStringArray(){
        return new byte[0];
    }


    public static DataInput createDataInput( ARRAY_TYPES streamType ) throws IOException{

        Object target = null;

        switch(streamType){
            case COMPRESSED_STRING_ARRAY:
                target = (byte[]) createCompressedStringArray();
                break;
            case EMPTY_STRING_ARRAY:
                target = (byte[]) createEmptyStringArray();
                break;
            case NULL:
                target = null;
                break;
            default:
                break;
        }

        try{
            InputStream inputStream = new ByteArrayInputStream((byte[])target);
            DataInput dataInput = new DataInputStream(inputStream);
            return dataInput;
        } catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }


    @ParameterizedTest
    @MethodSource
    public void testReadCompressedStringArray( DataInput dataInput , Object expectedResult){
        try{
            String[] res = WritableUtils.readCompressedStringArray(dataInput);
            assertArrayEquals((String[]) expectedResult, res);
        }
        catch (Exception e){
            assertEquals(expectedResult, e.getClass());
        }
    }


    
    private static Stream<Arguments> testReadCompressedStringArray() throws IOException{
        //      DataInput       ExpectedResult
        return Stream.of(       
                Arguments.of( createDataInput(ARRAY_TYPES.COMPRESSED_STRING_ARRAY) ,                new String[]{"foo"," ","string"," ","array"}),
                Arguments.of( createDataInput(ARRAY_TYPES.EMPTY_STRING_ARRAY),                      EOFException.class ),
                Arguments.of( createDataInput(ARRAY_TYPES.NULL),                                    NullPointerException.class )
        );
    }


    public enum ARRAY_TYPES{
        COMPRESSED_STRING_ARRAY,
        EMPTY_STRING_ARRAY,
        NULL
    }
}
