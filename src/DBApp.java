import java.io.File;
import java.util.Hashtable;

public class DBApp {
	
	private String defaultPath = "databases/";
	private String name, dbPath;
	
	// TODO:
	private int maxPageSize; 
	
	
    public DBApp(String name) {
    		this.name = name;
    		this.dbPath = defaultPath + this.name + '/';
    		File f = new File(dbPath);
    		f.mkdir();
    
    }
    
    public void createTable(String strTableName, String strClusteringKeyColumn, 
    		Hashtable<String,String> htblColNameType ) throws DBAppException {
    	
    		new Table(strTableName, dbPath, htblColNameType, maxPageSize);
    	
    }
}

class DBAppException extends Exception {

    /**
     * Any errors related to the DB App can be detected using this exceptions
     */
    private static final long serialVersionUID = 1L;

    public DBAppException(String string) { super(string); }

}