import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;

public class DBApp {

	private String defaultPath = "databases/";
	private String name, dbPath;

	// TODO: Review below attributes.
	private static HashSet<String> IndexedColumns = new HashSet<>(); // add all
																		// indexed
																		// columns
																		// here
	private Hashtable<String, String> htblColNameType;
	private HashMap<String, Table> tables = new HashMap<>();
	private FileWriter writer;

	private File metadata;
	private Properties properties;
	private Integer MaxRowsPerPage;

	public HashMap<String, Table> getTables() {
		return tables;
	}

	public DBApp(String name) throws IOException {

		Configuration config = new Configuration();
		this.name = name;
		this.dbPath = defaultPath + this.name + '/';
		this.MaxRowsPerPage = config.getMaximumSize();
		File dbFolder = new File(dbPath);
		dbFolder.mkdir();


		// Meta data file
		File data = new File(dbPath + "data/");
		data.mkdirs();

		this.metadata = new File(dbPath + "data/" + "metadata.csv");
		metadata.createNewFile();

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		File db = new File("databases/"+name+"/"+strTableName);
		if (db.exists()) {
			throw new DBAppException("Table " + strTableName + " is already exists");
		}

		Table table = new Table(strTableName, dbPath, htblColNameType, strClusteringKeyColumn);
		tables.put(strTableName, table);

		this.htblColNameType = htblColNameType;
		Set<String> columns = htblColNameType.keySet();

		for (String column : columns) {
			boolean key = false;
			boolean indexed = false;
			if (strClusteringKeyColumn.equals(column))
				key = true;
			if (IndexedColumns.contains(column))
				indexed = true;

			WriteMetaData(strTableName, column, htblColNameType.get(column), key, indexed);

		}
	}

	private void WriteMetaData(String strTableName, String column, String string, boolean key, boolean indexed)
			throws IOException, DBAppException {
		// TODO Auto-generated method stub

		writer = new FileWriter(metadata, true);
		writer.append(
				strTableName + "," + column + "," + htblColNameType.get(column) + "," + key + "," + indexed + '\n');

		writer.flush();
		writer.close();
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {

		Table table = getTable(strTableName);

		if (table == null)
			throw new DBAppException("Table: (" + strTableName + ") does not exist");
		if (!table.insert(htblColNameValue))
			throw new DBAppException("Insertion in table: (" + strTableName + ")failed");
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, ClassNotFoundException, IOException {

		Table table = getTable(strTableName);

		if (table == null)
			throw new DBAppException("Table: (" + strTableName + ") does not exist");
		if (!table.delete(htblColNameValue))
			throw new DBAppException("Deletion in table: (" + strTableName + ")failed");
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, ClassNotFoundException, IOException, ParseException {
		Table table = getTable(strTableName);

		if (table == null)
			throw new DBAppException("Table: (" + strTableName + ") does not exist");
		if (!table.update(strKey, htblColNameValue))
			throw new DBAppException("Update in table: (" + strTableName + ")failed");
	}

	private Table getTable(String strTableName) throws FileNotFoundException, IOException, ClassNotFoundException {

		File file = new File(dbPath + strTableName + "/" + strTableName + ".class");
		if (file.exists()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
			Table table = (Table) ois.readObject();
			ois.close();
			return table;
		}
		return null;
	}
	
	public void createBRINIndex(String strTableName,String strColName) throws FileNotFoundException, IOException, ClassNotFoundException, DBAppException 
	{
		// TODO Find the table and create respective index
		Table table = getTable(strTableName);
		table.createBRINIndex(strColName);
	}

}
