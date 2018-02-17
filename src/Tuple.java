import java.io.Serializable;


public class Tuple implements Serializable{
	
    private Object[] values;

    public Tuple (Object[] values) {
        this.values = values;
    }
    
    public String toString() {
		String result = "";
		for(Object o: values)
			result += o.toString() + ", ";
		return result;
	}

    public void add (int index, Object value) {
    		values[index] = value;
    }
    
    public Object[] get () {
        return  values;
    }
    

    
}
