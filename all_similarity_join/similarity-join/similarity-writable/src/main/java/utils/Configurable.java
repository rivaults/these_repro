package utils;

import org.apache.hadoop.conf.Configuration;

public interface Configurable {
    void setup(Configuration c);
}
