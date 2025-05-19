package writables.configuration;

import org.apache.hadoop.conf.Configuration;
import utils.Configurable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SequenceConfigurable implements Configurable {

    public static int MAX_SEQ = 2000;
    public static int MIN_SEQ = 150;
    public static int ALPHABET_SIZE = 4;
    public static final Map<Byte , Byte> ENCODER = new HashMap<>();

    @Override
    public void setup(Configuration c) {
        ENCODER.clear();
        MAX_SEQ = c.getInt("sequence.max.length", MAX_SEQ);
        MIN_SEQ = c.getInt("sequence.min.length", MIN_SEQ);
        byte[] alphabet = c.get("sequence.alphabet", "LMIVKREQASTNDFY").getBytes();
        ALPHABET_SIZE = c.getInt("sequence.alphabet.length", alphabet.length);
        for(int i = 0; i < alphabet.length; ++i){
            ENCODER.put(alphabet[i], (byte) (i));
        }
    }
}
