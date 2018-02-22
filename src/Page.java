import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

public class Page implements Serializable {

	private static final long serialVersionUID = 1L;

	private int maximumSize, tupleCount;
	private String path;
	private ArrayList<Tuple> tuples;

	public Page(String path) throws IOException {
		Configuration config = new Configuration();
		this.maximumSize = config.getMaximumSize();
		this.path = path;
		tuples = new ArrayList<>(maximumSize);

		// TODO:
		savePage();
	}

	// TODO:
	public boolean insert(Tuple tuple) throws DBAppException, IOException {

		if (isFull()) {
			return false;
		}
		tupleCount++;
		tuples.add(tuple);
		Collections.sort(tuples);
		savePage();
		return true;
	}
	public boolean delete(Tuple tuple) throws DBAppException, IOException {
		if(isEmpty())
			return false;
		tupleCount--;
		tuples.remove(tuple);
		savePage();
		return true;
		
	}

	public void savePage() throws IOException {
		File file = new File(path);
		if (!file.exists())
			file.createNewFile();
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
		oos.writeObject(this);
		oos.close();
	}

	public ArrayList<Tuple> getTuples() {
		return tuples;
	}

	public void setTuples(ArrayList<Tuple>tuples) {
		this.tuples = tuples;
	}

	public int getTupleCount() {
		return tupleCount;
	}

	public boolean isFull() {
		return tupleCount == maximumSize;
	}

	public void setTupleCount(int tupleCount) {
		this.tupleCount=tupleCount;
		
	}

	public boolean isEmpty() {
		return tupleCount==0;
	}
}
