import java.io.*;

public class Page {

    private int maximumSize;
    private String path;
    private int tupleCount;

    private Tuple[] tuples;

    private FileOutputStream fileOut;
    private ObjectOutputStream out;

    public Page(int maximumSize,String path) throws IOException {
        this.maximumSize = maximumSize;
        this.path = path;

        tuples = new Tuple[maximumSize];

        fileOut = new FileOutputStream(path);
        out = new ObjectOutputStream(fileOut);
    }

    public void insert (Tuple tuple) throws DBEngineException, IOException {
        if(tupleCount>=maximumSize){
            fileOut.close();
            out.close();
            throw new DBEngineException("Reached maximum Size");
        }
        tuples[tupleCount++] = tuple;
        out.writeObject(tuple);
    }

    public int getTupleCount() {
        return tupleCount;
    }
}
