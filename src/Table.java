import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Hashtable;

public class Table {

    private Hashtable<String, String> htblColNameType;
    private String tableName, path;
    private int curPageIndex, maxPageSize;
    private Page curPage;

    public Table(String tableName, String path,
                 Hashtable<String, String> htblColNameType,
                 int maxPageSize) throws DBAppException {
        this.htblColNameType = htblColNameType;
        this.tableName = tableName;
        this.path = path;
        this.maxPageSize = maxPageSize;
        curPageIndex = -1;

        File file = new File(path + tableName + '/');
        if (!file.exists()) {
            if (file.mkdir()) {
                System.out.println("Table directory created!");

            } else {
                throw new DBAppException("Failed to create table directory!");
            }
        }

    }

    public void insert(Tuple tuple) throws IOException , DBAppException{
        if (curPage == null || curPage.getTupleCount() >= maxPageSize)
            curPage = new Page(maxPageSize, path + "/" + tableName + "Page" + ++curPageIndex + ".class");
        curPage.insert(tuple);
    }
    
    public boolean checkValueType (Object value, String type) {
    		switch (type) {
    			case "java.lang.Integer": if (!(value instanceof Integer)) return false;
    			case "java.lang.String"	: if (!(value instanceof String)) return false;
    			case "java.lang.Double"	: if (!(value instanceof Double)) return false;
    			case "java.lang.Boolean": if (!(value instanceof Boolean)) return false;
    			case "java.util.Date"	: if (!(value instanceof Date)) return false;
    			default					: return true;
    		}
    }


}