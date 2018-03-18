import java.io.Serializable;
import java.util.Date;

/**
 * Created by ElMoghazy on 3/18/2018.
 */
public class IndexTuple implements Serializable, Comparable<IndexTuple> {

    private static final long serialVersionUID = 1L;

    public Object[] getValues() {
        return values;
    }

    private Object[] values;
    private int key;

    public IndexTuple(Object[] value, int key) {
        this.values = values;
        this.key = key; // the column to be indexed
    }


    @Override
    public String toString() {
        String result = "";
        for (Object o : values) {
            if (o != values[(values.length - 1)]) {
                result += o.toString() + ", ";
            } else {
                result += o.toString();
            }
        }
        return result;
    }

    public void add(int index, Object value) {
        values[index] = value;
    }

    public Object[] get() {
        return values;
    }
//Remove the Strings


    @Override
    public int compareTo(IndexTuple t) {

        switch ((values[key].getClass().getName())) {
            case "java.lang.Integer":
                return ((Integer) values[key]).compareTo(((Integer) t.values[t.key]));
            case "java.lang.String":
                return ((String) values[key]).compareTo(((String) t.values[t.key]));
            case "java.lang.Double":
                return ((Double) values[key]).compareTo(((Double) t.values[t.key]));
            case "java.lang.Boolean":
                return ((Boolean) values[key]).compareTo(((Boolean) t.values[t.key]));
            case "java.util.Date":
                return ((Date) values[key]).compareTo(((Date) t.values[t.key]));
        }


        return 0;
    }
}

