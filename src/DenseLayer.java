import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

public class DenseLayer implements Serializable {
	private String primarykey;
	private String indexkey;
	private String tableName,dataPath,indexPath, DenseLayerPath;
	private Hashtable<String, String> htblColNameType;
	private Table myTable;
	int noPages;
	
	public DenseLayer(String indexPath,Hashtable<String, String> htblColNameType,String indexkey,String primarykey,String dataPath,String tableName) throws IOException, ClassNotFoundException, DBAppException
	{
		this.primarykey=primarykey;
		this.indexkey=indexkey;
		this.htblColNameType=htblColNameType;
		this.indexPath=indexPath;
		this.dataPath = dataPath;
		this.tableName = tableName;
		DenseLayerPath=indexPath+"DenseLayer"+'/';
		noPages=-1;
		
		File tableSer = new File(dataPath + tableName + ".class");
		if (tableSer.exists()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tableSer));
			Table table = (Table) ois.readObject();
			ois.close();
			this.myTable = table;
		}
		
		createTDenseDirectory();
		load();
		saveindex();
	}
	
	private void createTDenseDirectory() 
	{
		File dense = new File(DenseLayerPath);
		dense.mkdir();
	}

	/*
	 *	Loads the data from the table	
	 * 	Sorts the data by the indexed column
	 * 	Stores the data in the dense layer
	 * 	
	 * */
	public void load() throws FileNotFoundException, IOException, ClassNotFoundException, DBAppException
	{
		ArrayList<Tuple> data = new ArrayList<Tuple>();
		int pageIndex = myTable.getCurPageIndex();
		for (int i = 0; i <= pageIndex; i++) 
		{
			// Student_0.class


			String name = dataPath + tableName + "_"+i+".class";

			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(name));
			Page page = (Page) ois.readObject();
			ois.close();
			for(Tuple t : page.getTuples())
			{
				int indexKeyPos = t.getIndex(indexkey) ;
				int primaryKeyPos = t.getIndex(primarykey);
				Object[] values = new Object[3];
				values[0] = t.get()[indexKeyPos]; 
				values[1] = t.get()[primaryKeyPos];
				values[2] = i;
				
				String[] types = new String[3];
				types[0] = t.getTypes()[indexKeyPos];
				types[1] = t.getTypes()[primaryKeyPos];
				types[2] = "java.lang.integer";
				
				String[] colName = new String[3];
				colName[0] = t.colName[indexKeyPos];
				colName[1] = t.colName[primaryKeyPos];
				colName[2] = "page.number";
				
				Tuple newTuple = new Tuple(values, types, colName, 0);
				
				data.add(newTuple);
			}
		}
		
		Collections.sort(data);
		
		Page curPage = createPage();
		for (int i = 0; i < data.size(); i++)
		{
			if(curPage.isFull())
			{
				curPage.savePage();
				curPage = createPage();
			}
			curPage.insert(data.get(i), true);
		}
		
	}
	 
    private Page createPage() throws IOException {

    	Page page = new Page(DenseLayerPath+indexkey +  "dense_" + (++noPages) + ".class");
    	saveindex();
        return page;
    }

    private void saveindex() throws IOException {
        File dense = new File(DenseLayerPath + "DenseLayer" + ".class");
        if (!dense.exists())
            dense.createNewFile();
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dense));
        oos.writeObject(this);
        oos.close();
    }
}