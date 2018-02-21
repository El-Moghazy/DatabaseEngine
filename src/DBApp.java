import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections;

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

	
	/**
	 * @param name 
	 * @param MaxRowsPerPage
	 * @throws IOException
	 */
	public DBApp(String name, Integer MaxRowsPerPage) throws IOException {

		this.name = name;
		this.dbPath = defaultPath + this.name + '/';
		this.MaxRowsPerPage = MaxRowsPerPage;
		File dbFolder = new File(dbPath);
		dbFolder.mkdir();

		// Configuration file
		properties = new Properties();
		properties.put("MaxRowsPerPage", MaxRowsPerPage.toString());
		new File(dbPath + "/config").mkdirs();
		File config = new File(dbPath + "/config/DBApp.config");
		config.createNewFile();
		FileOutputStream fos = new FileOutputStream(config);
		properties.store(fos, "DB Properties");
		fos.close();

		// Meta data file
		File data = new File(dbPath + "data/");
		data.mkdirs();

		this.metadata = new File(dbPath + "data/" + "metadata.csv");
		metadata.createNewFile();

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {

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
			throws IOException {
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

		Enumeration<String> keys = htblColNameValue.keys();

		while (keys.hasMoreElements()) {
			String param = keys.nextElement();
			if (table.getPrimaryKeyCheck().contains(param))
				throw new DBAppException("Insertion in table: (" + strTableName
						+ ")failed. PrimaryKey value already exsists in the tabe");
		}
		if (table == null)
			throw new DBAppException("Table: (" + strTableName + ") does not exist");
		if (!table.insert(htblColNameValue))
			throw new DBAppException("Insertion in table: (" + strTableName + ")failed");
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

}
