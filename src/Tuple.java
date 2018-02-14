
public class Tuple {
	
    private Object[] values;

    public Tuple (Object[] values) {
        this.values = values;
    }

    public void add (int index, Object value) {
    		values[index] = value;
    }
    
    public Object[] get () {
        return  values;
    }

    
}
