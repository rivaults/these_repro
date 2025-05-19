package utils;

import reader.SequenceLineNumberedInput;
import writables.IdentifiedWritable;
import writables.SequenceWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import java.lang.reflect.InvocationTargetException;


public class SetupGenericsInput {

    public static Class<? extends IdentifiedWritable> OBJECT_CLS;
    public static Class<? extends InputFormat> READER_OBJECT_CLS;

    public static void setup(Configuration c) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        OBJECT_CLS = c.getClass("object.class", SequenceWritable.class, IdentifiedWritable.class);
        READER_OBJECT_CLS = c.getClass("object.reader.class", SequenceLineNumberedInput.class, InputFormat.class);
        OBJECT_CLS.getDeclaredConstructor().newInstance().setup(c);
    }
}
