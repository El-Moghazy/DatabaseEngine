import java.io.File;
import java.io.IOException;
import java.util.Hashtable;


public class BrinIndex {
	
	private String indexPath, dataPath , tableName;
	private DenseLayer denseLayer;
	private BrinLayer brinLayer;
	// Constructor
	public BrinIndex(String dataPath,Hashtable<String, String> htblColNameType,String indexkey,String primarykey,String tableName) throws IOException, ClassNotFoundException, DBAppException
	{	
		this.dataPath=dataPath;
		indexPath = dataPath+indexkey+'/';
		this.tableName = tableName;
		
		createDenseIndex(indexPath,htblColNameType,indexkey,primarykey);
		createBrinIndex();
		createTIndexDirectory();
	}
	
	// 
	public void createDenseIndex(String indexPath,Hashtable<String, String> htblColNameType,String indexkey,String primarykey) throws IOException, ClassNotFoundException, DBAppException
	{
		denseLayer=new DenseLayer(indexPath, htblColNameType, indexkey, primarykey,dataPath,tableName);
	}
	
	public void createBrinIndex() throws IOException
	{
		brinLayer = new BrinLayer(indexPath);
	}
	private void createTIndexDirectory() {
        File brin = new File(indexPath);
        brin.mkdir();
    }
}
