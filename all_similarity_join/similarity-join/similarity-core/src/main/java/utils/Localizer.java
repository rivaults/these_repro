package utils;

import writables.partitionned.LocalizedIdWritable;

public class Localizer {

    private final LocalizedIdWritable out = new LocalizedIdWritable();

    private int splitId;
    private int localization;

    public Localizer() {
        localization = -1;
        splitId = -1;
    }

    public int getSplitId() {
        return splitId;
    }

    public void setSplitId(int splitId) {
        this.splitId = splitId;
    }

    public int getLocalization() {
        return localization;
    }

    public void reset(int id){
        splitId = id;
        localization = 0;
    }

    public void next(){
        localization += 1;
    }

    public LocalizedIdWritable getLocalizable(long id){
        out.setId(id);
        out.setSplitId(splitId);
        out.setLocalization(localization);
        return out;
    }
}
