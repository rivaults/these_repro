package utils;

import writables.IdentifiedWritable;
import writables.partitionned.GenericKeysObjectWritable;

public class Factory {

    private final Class<? extends GenericKeysObjectWritable> keyCls;
    private final Class<? extends IdentifiedWritable> objCls;

    public Factory(Class<? extends IdentifiedWritable> objCls,
                   Class<? extends GenericKeysObjectWritable> keyCls) {
        this.keyCls = keyCls;
        this.objCls = objCls;
    }

    public Class<? extends GenericKeysObjectWritable> getObjectKeysClass(){
        return keyCls;
    }

    public Class<? extends IdentifiedWritable> getObjectClass(){
        return objCls;
    }

    public GenericKeysObjectWritable getObjectKeysInstance() throws InstantiationException, IllegalAccessException {
        return keyCls.newInstance();
    }

    public IdentifiedWritable getObjectInstance() throws InstantiationException, IllegalAccessException {
        return objCls.newInstance();
    }

}
