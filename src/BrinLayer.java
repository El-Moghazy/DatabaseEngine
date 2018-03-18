import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class BrinLayer {
	private String indexPath, BrinLayerPath;
	private int noPages;
	
	public BrinLayer(String indexPath) throws IOException{
		this.indexPath=indexPath;
		BrinLayerPath=indexPath+"BrinLayer"+'/';
		noPages=0;
		createTBrineDirectory();
		createPage();
		saveindex();
	}
	 private void createTBrineDirectory() {
	        File brin = new File(BrinLayerPath);
	        brin.mkdir();
	    }

	    private Page createPage() throws IOException {
	        //TODO
	    	return null;
	    }

	    private void saveindex() throws IOException {
	        File brin = new File(BrinLayerPath + "BrinLayer" + ".class");
	        if (!brin.exists())
	            brin.createNewFile();
	        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(brin));
	        oos.writeObject(this);
	        oos.close();
	    }

}
