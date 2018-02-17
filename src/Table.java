import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;



public class Table {

	private Hashtable<String, String> htblColNameType;
	private String tableName, path,clustringKey;
	private int curPageIndex, maxPageSize,numOfCols;
	private Page curPage;

	public Table(String tableName, String path, Hashtable<String, String> htblColNameType,
			String strClusteringKeyColumn, int maxPageSize) 
					throws DBAppException, IOException 
					{
		this.htblColNameType = htblColNameType;
		this.tableName = tableName;
		this.path = path;
		this.maxPageSize = maxPageSize;
		curPageIndex = -1;
		clustringKey=strClusteringKeyColumn;

		createTableDirectory();
		createPage();
		saveTable();
					}

	private void saveTable() throws IOException
	{
		File file = new File(path+tableName+".class");
		if(!file.exists())
			file.createNewFile();
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
		oos.writeObject(this);
		oos.close();
	}
	
	public boolean insert(Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {
		
		checkInsertedColumns(htblColNameValue);
		
		Object value = htblColNameValue.get(clustringKey);
		if (value == null)
			throw new DBAppException("Clustering key is not allowed to be null");
		
		Object[] values = new Object[numOfCols];
		Set<String> columns = htblColNameValue.keySet();
    	int i=0;
		for(String column: columns){
			values[i]= htblColNameValue.get(column);
			i++;
		}
		
		insertTuple(new Tuple(values));
		saveTable();
		return true;
	
	}
	private void checkInsertedColumns(Hashtable<String, Object> htblColNameValue) throws DBAppException 
	{
		for (Entry<String, Object> entry : htblColNameValue.entrySet()) 
		{
			String colName = entry.getKey();
			if (!htblColNameType.containsKey(colName))
				throw new DBAppException("Column : "+ colName + " doesn't exist");
			if(!checkValueType(entry.getValue(), htblColNameType.get(colName)))
				throw new DBAppException("Type mismatch on column : "+ colName);
		}
	}

	public Page insertTuple(Tuple tuple) throws IOException , DBAppException, ClassNotFoundException{
		/*if (curPage == null || curPage.getTupleCount() >= maxPageSize)
			curPage = new Page(maxPageSize, path + "/" + tableName + "Page" + ++curPageIndex + ".class");
		curPage.insert(tuple);*/
		
		File file= new File(path + tableName + "_" + curPageIndex+".class");
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
		Page cuPage  = (Page)ois.readObject();
		if(curPage.isFull())
			curPage = createPage();
		curPage.insert(tuple);
		ois.close();
		return curPage;
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
	private void createTableDirectory(){
		File file = new File(path);
		file.mkdir();
	}
	private Page createPage() throws IOException{
		Page page = new Page(maxPageSize, path+tableName+"_"+ curPageIndex+".class");
		curPageIndex++;
		saveTable();
		return page;
	}
	


}