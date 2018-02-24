import java.io.Serializable;
import java.util.Date;

public class Tuple implements Serializable ,Comparable<Tuple>{

	private static final long serialVersionUID = 1L;
	private Object[] values;
	private String[] types;
	private int key;

	public Tuple(Object[] values,String[] types,int key) {
		this.values = values;
		this.types=types;
		this.key=key;
	}

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
	@Override
	public boolean equals(Object o){
		Tuple t = (Tuple) o;
		boolean equal= true;
		for(int i=0;i<t.values.length-1;i++){
			System.out.println(i);
			switch (types[i].toLowerCase()) {
			case "java.lang.integer":
				if(!((Integer)values[i]).equals((Integer)t.values[i]))
				equal=false;break;
			case "java.lang.string":
				if(!((String)values[i]).equals((String)t.values[i]))
				equal=false;break;
			case "java.lang.double":
				if(!((Double)values[i]).equals((Double)t.values[i]))
				equal=false;break;
			case "java.lang.boolean":
				if(!((Boolean)values[i]).equals((Boolean)t.values[i]))
				equal=false;break;
			case "java.util.date":
				if(!((Date)values[i]).equals((Date)t.values[i]))
				equal=false;break;
			}
		}
			return equal;
	}
	@Override
	public int compareTo(Tuple t) {
		boolean equal= true;
		for(int i=0;i<t.values.length-1;i++){
			switch (types[i].toLowerCase()) {
			case "java.lang.integer":
				if(!((Integer)values[i]).equals((Integer)t.values[i]))
				equal=false;break;
			case "java.lang.string":
				if(!((String)values[i]).equals((String)t.values[i]))
				equal=false;break;
			case "java.lang.double":
				if(!((Double)values[i]).equals((Double)t.values[i]))
				equal=false;break;
			case "java.lang.boolean":
				if(!((Boolean)values[i]).equals((Boolean)t.values[i]))
				equal=false;break;
			case "java.util.date":
				if(!((Date)values[i]).equals((Date)t.values[i]))
				equal=false;break;
			}
		}
		if(equal)
			return 0;
		switch (types[key].toLowerCase()) {
		case "java.lang.integer":
			return ((Integer)values[key]).compareTo(((Integer)t.values[t.key]));
		case "java.lang.string":
			return ((String)values[key]).compareTo(((String)t.values[t.key]));
		case "java.lang.double":
			return ((Double)values[key]).compareTo(((Double)t.values[t.key]));
		case "java.lang.boolean":
			return ((Boolean)values[key]).compareTo(((Boolean)t.values[t.key]));
		case "java.util.date":
			return ((Date)values[key]).compareTo(((Date)t.values[t.key]));
		}
		return 0;
	}

	public Tuple Clone() {
		return new Tuple(values, types, key);
	}
}
