package org.apache.storm.utils;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.awt.image.BufferedImage;

import javax.imageio.ImageIO;
import javax.imageio.stream.FileImageOutputStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;



public class TestWritableUtilsReadCompressedByteArray {


    private static Stream<Arguments> testReadCompressedByteArray() throws IOException{
        //      DataInput       ExpectedResult
        return Stream.of(       
                // Test Suite Minimale
                Arguments.of( createDataInput(STREAM_TYPES.FILE) ,                          "foo file text!".getBytes()),
                Arguments.of( createDataInput(STREAM_TYPES.BYTE_ARRAY),                     "foo byte array text!".getBytes() ),
                Arguments.of( createDataInput(STREAM_TYPES.EMPTY_BYTE_ARRAY),                EOFException.class ),
                Arguments.of( null,                                             NullPointerException.class ),
                // Control Flow Coverage
                Arguments.of( createDataInput(STREAM_TYPES.EMPTY_FORMATTED_BYTE_ARRAY),      null )
        );
    }

    public void createCompressedImageFile() {
        
        BufferedImage image = new BufferedImage(10, 10, BufferedImage.TYPE_INT_RGB);
        String path = ".." + File.pathSeparator + "image.jpg";
        try {
            FileImageOutputStream imout = new FileImageOutputStream( new File(path) );
            ImageIO.write(image,"jpg", imout);
            imout.flush();
            imout.close();
        } catch (IOException ex) {
          System.err.println("I/O error : Cannot generate " + path);
        }
        
      }
      
    public static File createCompressedFile() throws IOException{
        File f = new File(".." + File.pathSeparator + "filename" );
        FileOutputStream fos;
        DataOutputStream dos;
        ByteArrayOutputStream baos;
        GZIPOutputStream gzos;

        try{
            fos =   new FileOutputStream(f);
            dos =   new DataOutputStream(fos);

            baos =   new ByteArrayOutputStream();
            gzos =  new GZIPOutputStream(baos);

            gzos.write("foo file text!".getBytes());
            gzos.close();
            byte[] compressedText = baos.toByteArray();
            baos.close();

            dos.writeInt(compressedText.length);
            dos.write(compressedText);

            dos.close();
            fos.close();
        } catch( Exception e){
            e.printStackTrace();
        }
        return f;
    }

    public static File createEmptyFile() throws IOException{
        File f = new File(".." + File.pathSeparator + "filename" );
        return f;
    }

    public static byte[] createCompressedByteArray() throws IOException{
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

            gzos.write("foo byte array text!".getBytes());
            gzos.close();
            compressed = baos.toByteArray();
            baos.close();

            dos.writeInt(compressed.length);
            dos.write(compressed);
            dos.close();
            compressed = baos2.toByteArray();
            baos2.close();
        } catch( Exception e){
            e.printStackTrace();
        }
        return compressed;
    }

    public static byte[] createEmptyCompressedByteArray() throws IOException{
        byte[] compressed =     null;
        ByteArrayOutputStream   baos2;
        DataOutputStream        dos;
        try{
            baos2 = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos2);
            dos.writeInt(-1);
            compressed = baos2.toByteArray();
            baos2.close();
        } catch( Exception e){
            e.printStackTrace();
        }
        return compressed;
    }

    public static byte[] createEmptyByteArray(){
        return new byte[32];
    }


    public static DataInput createDataInput( STREAM_TYPES streamType ) throws IOException{

        Object target = null;

        switch(streamType){
            case FILE:
                target = (File) createCompressedFile();
                break;
            case BYTE_ARRAY:
                target = (byte[]) createCompressedByteArray();
                break;
            case EMPTY_FILE:
                target = (File) createEmptyFile();
                break;
            case EMPTY_BYTE_ARRAY:
                target = (byte[]) createEmptyByteArray();
                break;
            case EMPTY_FORMATTED_BYTE_ARRAY:
                target = (byte[]) createEmptyCompressedByteArray();
                break;
            default:
                break;
        }

        try{
            InputStream inputStream = StreamFactory.createInputStream(streamType, target);
            DataInput dataInput = new DataInputStream(inputStream);
            return dataInput;
        } catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }


    @ParameterizedTest
    @MethodSource
    public void testReadCompressedByteArray( DataInput dataInput , Object expectedResult){
        try{
            byte[] res = WritableUtils.readCompressedByteArray(dataInput);
            if (res == null)    assertEquals(expectedResult, null);
            else                assertArrayEquals((byte[])expectedResult, res);
        }
        catch (Exception e){
            assertEquals(expectedResult, e.getClass());
        }
    }


    public enum STREAM_TYPES{
        FILE,
        BYTE_ARRAY,
        EMPTY_FILE,
        EMPTY_BYTE_ARRAY,
        EMPTY_FORMATTED_BYTE_ARRAY
    }

    public static class StreamFactory{
        public static InputStream createInputStream( STREAM_TYPES streamtype, Object target ) throws FileNotFoundException{
            switch(streamtype){
                case FILE:
                    return new FileInputStream( (File) target );
                case EMPTY_FILE:
                    return new FileInputStream( (File) target );
                case BYTE_ARRAY:
                    return new ByteArrayInputStream( (byte[]) target );
                case EMPTY_BYTE_ARRAY:
                    return new ByteArrayInputStream( (byte[]) target );
                case EMPTY_FORMATTED_BYTE_ARRAY:
                    return new ByteArrayInputStream( (byte[]) target );
                default:
                    return null;
            }
        }
    }
}
