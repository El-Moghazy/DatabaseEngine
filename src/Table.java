import java.io.File;
import java.io.IOException;
import java.util.Hashtable;

public class Table {

    private Hashtable<String, String> htblColNameType;
    private String tableName;
    private String path;
    private int curPageIndex;
    private int maxPageSize;
    private Page curPage;

    public Table(String tableName, String path,
                 Hashtable<String, String> htblColNameType,
                 int maxPageSize) throws DBEngineException {
        this.htblColNameType = htblColNameType;
        this.tableName = tableName;
        this.path = path;
        this.maxPageSize = maxPageSize;
        curPageIndex = -1;

        File file = new File(path + tableName);
        if (!file.exists()) {
            if (file.mkdir()) {
                System.out.println("Table directory created!");

            } else {
                throw new DBEngineException("Failed to create table directory!");
            }
        }

    }

    public void insert(Tuple tuple) throws IOException ,DBEngineException{
        if (curPage == null || curPage.getTupleCount()>=maxPageSize)
            curPage = new Page(maxPageSize, path + "/" + tableName + "Page" + ++curPageIndex + ".class");
        curPage.insert(tuple);
    }
}