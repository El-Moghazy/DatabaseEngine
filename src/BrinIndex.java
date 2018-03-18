import java.io.IOException;
import java.util.Hashtable;


public class BrinIndex {
	
	private String indexPath, dataPath;
	private DenseLayer denseLayer;
	private BrinLayer brinLayer;
	// Constructor
	public BrinIndex(String dataPath,Hashtable<String, String> htblColNameType,String indexkey,String primarykey) throws IOException
	{	
		this.dataPath=dataPath;
		indexPath = dataPath+indexkey+'/';
		
		createDenseIndex(indexPath,htblColNameType,indexkey,primarykey);
		createBrinIndex();
	}
	
	// 
	public void createDenseIndex(String indexPath,Hashtable<String, String> htblColNameType,String indexkey,String primarykey) throws IOException
	{
		denseLayer=new DenseLayer(indexPath, htblColNameType, indexkey, primarykey);
	}
	
	public void createBrinIndex() throws IOException
	{
		brinLayer = new BrinLayer(indexPath);
	}
	
}
