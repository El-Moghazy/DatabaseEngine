import java.io.*;

public class Page {

    private int maximumSize, tupleCount;
    private String path;
    private Tuple[] tuples;

    private FileOutputStream fileOut;
    private ObjectOutputStream out;

    public Page (int maximumSize, String path) throws IOException {
        this.maximumSize = maximumSize;
        this.path = path;

        tuples = new Tuple[maximumSize];

        // TODO: FIRST => make new File!? File f = new File(path); if(!f.exist()) {f.createNewFile();}
        fileOut = new FileOutputStream(path);
        out = new ObjectOutputStream(fileOut);
    }

    // TODO: make it boolean to know if the tuple is inserted
    public void insert (Tuple tuple) throws DBAppException, IOException {
        if (tupleCount >= maximumSize){
            fileOut.close();
            out.close();
            throw new DBAppException("Reached maximum Size");
        }
        tuples[tupleCount++] = tuple;
        out.writeObject(tuple);
        out.close();
    }

    public int getTupleCount() {
        return tupleCount;
    }
}
