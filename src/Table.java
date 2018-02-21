import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Calendar;

public class Table implements Serializable {

	private static final long serialVersionUID = 1L;
	private ArrayList<Object> PrimaryKeyCheck;

	public ArrayList<Object> getPrimaryKeyCheck() {
		return PrimaryKeyCheck;
	}

	private Hashtable<String, String> htblColNameType;
	private String tableName, path, strClusteringKeyColumn;
	private int curPageIndex, maxPageSize, numOfCols;

	public Table(String tableName, String path, Hashtable<String, String> htblColNameType,
			String strClusteringKeyColumn) throws DBAppException, IOException {
		Configuration config = new Configuration();
		PrimaryKeyCheck = new ArrayList();
		this.maxPageSize =  config.getMaximumSize();
		this.tableName = tableName;
		this.path = path + tableName + '/';
		this.htblColNameType = htblColNameType;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		numOfCols = htblColNameType.size();
		curPageIndex = -1;

		createTableDirectory();
		createPage();
		saveTable();

	}

	public boolean insert(Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ClassNotFoundException, IOException {

		checkInsertedColumns(htblColNameValue);

		Object value = htblColNameValue.get(strClusteringKeyColumn);
		if (value == null)
			throw new DBAppException("Clustering key is not allowed to be null");

		Object[] values = new Object[numOfCols+1];
		Set<String> columns = htblColNameValue.keySet();
		int i = 0;

		for (String column : columns)
			values[i++] = htblColNameValue.get(column);
		Date d = Calendar.getInstance().getTime();
		values[numOfCols]=d;
		if ( PrimaryKeyCheck.contains(value)) {
			throw new DBAppException("Insertion in table failed. PrimaryKey value already exsists in the tabe");
		}
		
		insertTuple(new Tuple(values));
		PrimaryKeyCheck.add(value);
		saveTable();
		return true;

	}

	public String getStrClusteringKeyColumn() {
		return strClusteringKeyColumn;
	}

	public Page insertTuple(Tuple tuple) throws IOException, DBAppException, ClassNotFoundException {

		File file = new File(path + tableName + "_" + curPageIndex + ".class");
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
		Page curPage = (Page) ois.readObject();

		if (curPage.isFull())
			curPage = createPage();
		curPage.insert(tuple);
		ois.close();
		return curPage;
	}

	private void checkInsertedColumns(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		for (Entry<String, Object> entry : htblColNameValue.entrySet()) {
			String colName = entry.getKey();
			if (!htblColNameType.containsKey(colName))
				throw new DBAppException("Column : " + colName + " doesn't exist");
			if (checkValueType(entry.getValue(), htblColNameType.get(colName)))
				throw new DBAppException("Type mismatch on column : " + colName);
		}
	}

	public boolean checkValueType(Object value, String type) {

		switch (type.toLowerCase()) {
		case "java.lang.integer":
			if (!(value instanceof Integer))
				return false;
		case "java.lang.string":
			if (!(value instanceof String))
				return false;
		case "java.lang.double":
			if (!(value instanceof Double))
				return false;
		case "java.lang.boolean":
			if (!(value instanceof Boolean))
				return false;
		case "java.util.date":
			if (!(value instanceof Date))
				return false;
		default:
			return true;
		}
	}

	private void createTableDirectory() {
		File table = new File(path);
		table.mkdir();
	}

	private Page createPage() throws IOException {
		Page page = new Page(path + tableName + "_" + (++curPageIndex) + ".class");
		saveTable();
		return page;
	}

	private void saveTable() throws IOException {
		File table = new File(path + tableName + ".class");
		if (!table.exists())
			table.createNewFile();
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(table));
		oos.writeObject(this);
		oos.close();
	}

}
