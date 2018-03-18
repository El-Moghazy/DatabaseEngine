import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Hashtable;

public class DenseLayer {
	private String primarykey;
	private String indexkey;
	private String indexPath, DenseLayerPath;
	private Hashtable<String, String> htblColNameType;
	int noPages;
	public DenseLayer(String indexPath,Hashtable<String, String> htblColNameType,String indexkey,String primarykey) throws IOException
	{
		this.primarykey=primarykey;
		this.indexkey=indexkey;
		this.htblColNameType=htblColNameType;
		this.indexPath=indexPath;
		DenseLayerPath=indexPath+"DenseLayer"+'/';
		noPages=0;
		createTDenseDirectory();
		createPage();
		saveindex();
	}
	
	 private void createTDenseDirectory() {
	        File dense = new File(DenseLayerPath);
	        dense.mkdir();
	    }

	    private Page createPage() throws IOException {
	        //TODO
	    	return null;
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