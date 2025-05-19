package mrjobs.mrsf;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import utils.LocalizerReader;
import utils.SetupTools;
import writables.AdaptiveKeyWritable;
import writables.IdentifiedWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class VerifyMapper extends Mapper<NullWritable, IdentifiedWritable, AdaptiveKeyWritable, IdentifiedWritable> {

    private final LocalizerReader reader = new LocalizerReader();
    private final ArrayList<Long> out = new ArrayList<>();
    private AdaptiveKeyWritable outK;

    @Override
    public void setup(Context context) throws IOException {
        try {
            SetupTools.setup(context.getConfiguration());
            reader.initialize(context, out);
            outK = new AdaptiveKeyWritable();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void map(NullWritable n, IdentifiedWritable outV, Context context)
            throws IOException, InterruptedException {
        boolean toSend = reader.next(out);
        if (toSend) {
            for (long transfer : out){
                outK.setAll(transfer, (short) 0, (byte) ((transfer == outV.getId()) ? 0 : 1));
                context.write(outK, outV);
            }
        }
    }
}
