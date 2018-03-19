import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;


public class BrinIndex implements Serializable{
	
	private String indexPath, dataPath , tableName , indexColName;
	private transient DenseLayer denseLayer;
	private transient BrinLayer brinLayer;
	
	// Constructor
	public BrinIndex(String dataPath,Hashtable<String, String> htblColNameType,String indexkey,String primarykey,String tableName) throws IOException, ClassNotFoundException, DBAppException
	{	
		this.dataPath=dataPath;
		indexPath = dataPath+indexkey+'/';
		this.tableName = tableName;
		indexColName = indexkey;

		createTIndexDirectory();
		createDenseIndex(indexPath,htblColNameType,indexkey,primarykey);
		createBrinIndex(indexkey);
		save();

	}
	
	// 
	public void createDenseIndex(String indexPath,Hashtable<String, String> htblColNameType,String indexkey,String primarykey) throws IOException, ClassNotFoundException, DBAppException
	{
		denseLayer=new DenseLayer(indexPath, htblColNameType, indexkey, primarykey,dataPath,tableName);
	}
	
	public void createBrinIndex(String indexkey) throws IOException, ClassNotFoundException, DBAppException
	{
		brinLayer = new BrinLayer(indexPath,indexkey);
	}
	
	private void createTIndexDirectory() {
        File brin = new File(indexPath);
        brin.mkdir();
    }

	public String getIndexColName() {
		return indexColName;
	}
	
	public DenseLayer fetchDenseLayer() throws FileNotFoundException, IOException, ClassNotFoundException
	{
		File file = new File(indexPath + "DenseLayer/DenseLayer.class");
		if (file.exists()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
			DenseLayer layer = (DenseLayer) ois.readObject();
			ois.close();
			return denseLayer = layer;
		}
		return null;
	}
	
	public BrinLayer fetchBrinLayer() throws FileNotFoundException, IOException, ClassNotFoundException
	{
		File file = new File(indexPath + "BrinLayer/BrinLayer.class");
		if (file.exists()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
			BrinLayer layer = (BrinLayer) ois.readObject();
			ois.close();
			return brinLayer = layer;
		}
		return null;
	}
	
	public void save() throws IOException
	{
		File index = new File(indexPath + indexColName + ".class");
        if (!index.exists())
            index.createNewFile();
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(index));
        oos.writeObject(this);
        oos.close();
	}
	public Iterator<Tuple> search(Object min,Object max,boolean minEq,boolean maxEq) throws FileNotFoundException, ClassNotFoundException, IOException{
			ArrayList<Integer >pages=brinLayer.search(min,max,minEq,maxEq);
		
		return denseLayer.search(min,max,minEq,maxEq,pages);
		
	}

	public void deleteTuple(Tuple tupleToDelete) throws FileNotFoundException, ClassNotFoundException, IOException, DBAppException
	{
		fetchBrinLayer();
		fetchDenseLayer();
		// Read page number from brin layer
		ArrayList<Integer> list = new ArrayList<Integer>();
		Object idx = tupleToDelete.get()[tupleToDelete.getIndex(indexColName)];
		list = brinLayer.search(idx, idx, true, true);
		if(list.isEmpty())
		{
			System.err.println("Delete-trace: Tuple doesn't exist in index");
			return;
		}
		int pageNumber = list.get(0);
		
		denseLayer.delete(tupleToDelete, pageNumber);
		brinLayer.refresh(pageNumber,denseLayer.noPages);
	}

	public void insertTuple(Tuple t, int pagetable) throws FileNotFoundException, ClassNotFoundException, IOException, DBAppException {
		fetchBrinLayer();
		fetchDenseLayer();
		ArrayList<Integer> list = new ArrayList<Integer>();
		Object idx = t.get()[t.getIndex(indexColName)];
		list = brinLayer.search(idx, idx, true, true);
		int page;
		if(list.isEmpty())
			page=denseLayer.noPages;
		else
			page=list.get(0);
		page=denseLayer.insert(t, page,pagetable);
		brinLayer.refresh(page,denseLayer.noPages);
		
		
		
	}

}
