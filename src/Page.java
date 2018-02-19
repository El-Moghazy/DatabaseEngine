import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Page implements Serializable {

	private static final long serialVersionUID = 1L;

	private int maximumSize, tupleCount;
	private String path;
	private Tuple[] tuples;

	public Page(String path) throws IOException {
		Configuration config = new Configuration();
		this.maximumSize = config.getMaximumSize();
		this.path = path;
		tuples = new Tuple[maximumSize];

		// TODO:
		savePage();
	}

	// TODO:
	public boolean insert(Tuple tuple) throws DBAppException, IOException {

		if (isFull()) {
			return false;
		}

		tuples[tupleCount++] = tuple;
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

	public Tuple[] getTuples() {
		return tuples;
	}

	public void setTuples(Tuple[] tuples) {
		this.tuples = tuples;
	}

	public int getTupleCount() {
		return tupleCount;
	}

	public boolean isFull() {
		return tupleCount == maximumSize;
	}

}
