import java.io.*;

public class Page implements Serializable {

    private int maximumSize, tupleCount;
    private String path;
    private Tuple[] tuples;
/*
 * 
 * No need to make these variables global ?No need to have them aslan ?
    private FileOutputStream fileOut;
    private ObjectOutputStream out;
*/
    public Page (int maximumSize, String path) throws IOException {
        this.maximumSize = maximumSize;
        this.path = path;
        tuples = new Tuple[maximumSize];

        // TODO: FIRST => make new File!? File f = new File(path); if(!f.exist()) {f.createNewFile();}
        savePage();
    }

    // TODO: make it boolean to know if the tuple is inserted
    public boolean insert (Tuple tuple) throws DBAppException, IOException {
        if (isFull()){
            //fileOut.close();
            //out.close();
            //throw new DBAppException("Reached maximum Size");
        	return false;
        }
        tuples[tupleCount++] = tuple;
        savePage();
        return true;
    }
    public void savePage() throws IOException {
    	File file = new File(path);
    	if(!file.exists())
    		file.createNewFile();
    	ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
    	oos.writeObject(this);
    	oos.close();
    }
    public int getTupleCount() {
        return tupleCount;
    }
    public boolean isFull(){
    	return tupleCount==maximumSize;
    }
}
