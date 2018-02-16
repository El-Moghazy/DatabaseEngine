import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

public class DBApp {
	
	private String defaultPath = "databases/";
	private String name, dbPath;
	private Table table;
	private static HashSet<String> IndexedColumns= new HashSet<>(); // add all indexed columns here
	private Hashtable<String,String> htblColNameType ;
	
	// TODO:
	private int maxPageSize; 
	
	
    public DBApp(String name) {
    		this.name = name;
    		this.dbPath = defaultPath + this.name + '/';
    		File f = new File(dbPath);
    		f.mkdir();
    
    }
    
    public void createTable(String strTableName, String strClusteringKeyColumn, 
    		Hashtable<String,String> htblColNameType ) throws DBAppException, IOException {
    	
    		table = new Table(strTableName, dbPath, htblColNameType, maxPageSize);
    		this.htblColNameType=htblColNameType;
    		FileWriter writer = new FileWriter(strTableName+"'s_metadata.csv");
    		Set<String> columns = htblColNameType.keySet();
    		for(String column: columns){
    			boolean key=false;
    			boolean indexed=false;
    			if(strClusteringKeyColumn.equals(column))
    				key=true;
    			if(IndexedColumns.contains(column))
    				indexed=true;
    			//there is no indexed columns for now
    			writer.append(strTableName+","+column+","+htblColNameType.get(column)+","+key+","+indexed+'\n');
    		}
    		writer.close();
    }
    public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue)throws DBAppException, IOException
    {
    	Set<String> columns = htblColNameValue.keySet();
    	Object[] values= new Object[htblColNameValue.size()];
    	int i=0;
		for(String column: columns){
			if(!table.checkValueType(htblColNameValue.get(column), htblColNameType.get(column)))
				throw new DBAppException("that is not the type of the column");
			values[i]= htblColNameValue.get(column);
			i++;
		}
		table.insert(new Tuple(values));
		
    }
    
}

class DBAppException extends Exception {

    /**
     * Any errors related to the DB App can be detected using this exceptions
     */
    private static final long serialVersionUID = 1L;

    public DBAppException(String string) { super(string); }

}
