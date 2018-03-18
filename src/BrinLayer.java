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

public class BrinLayer implements Serializable {
	private String indexPath, BrinLayerPath;
	private String indexkey;
	private int noPages;
	
	public BrinLayer(String indexPath,String indexkey) throws IOException, ClassNotFoundException, DBAppException{
		this.indexkey=indexkey;
		this.indexPath=indexPath;
		BrinLayerPath=indexPath+"BrinLayer"+'/';
		noPages=-1;
		createTBrineDirectory();
		load();
		saveindex();
	}
	 private void createTBrineDirectory() {
	        File brin = new File(BrinLayerPath);
	        brin.mkdir();
	    }
	 public void load() throws FileNotFoundException, IOException, ClassNotFoundException, DBAppException
		{
		 File dense = new File(indexPath+ "DenseLayer"+ '/' + "DenseLayer" + ".class");
			if (!dense.exists()) 
				throw new DBAppException("dense does not exist");
				ObjectInputStream ois2 = new ObjectInputStream(new FileInputStream(dense));
				DenseLayer ddense = (DenseLayer) ois2.readObject();
				ois2.close();
			
				Page curPage = createPage();
			for (int i = 0; i <= ddense.noPages; i++) 
			{
				// Student_0.class
				System.out.println( indexPath+ "DenseLayer"+ '/' + indexkey+"dense" + "_"+i+".class");
				String name = indexPath+ "DenseLayer"+ '/' + indexkey+"dense" + "_"+i+".class";
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(name));
				Page page = (Page) ois.readObject();
				ois.close();
				
				int length = page.getTuples().size();
				
				Object[] values = new Object[3];
				values[0] =  page.getTuples().get(0).get()[0];
				values[1] = page.getTuples().get(length-1).get()[0];
				values[2] = i;
				
				String[] types = new String[3];
				types[0] = page.getTuples().get(0).getTypes()[0];
				types[1] = page.getTuples().get(length-1).getTypes()[0];
				types[2] = "java.lang.integer";
				
				String[] colName = new String[3];
				colName[0] = page.getTuples().get(0).colName[0];
				colName[1] = page.getTuples().get(0).colName[0];
				colName[2] = "page.number";
				Tuple tuple=new Tuple(values, types, colName, 0);
				if(curPage.isFull())
					curPage = createPage();
				curPage.insert(tuple, false);
			}
			
			
			
			
		}

	    private Page createPage() throws IOException {
	    	Page page = new Page(BrinLayerPath+ indexkey +  "dense_" + (++noPages) + ".class");
	    	saveindex();
	        return page;
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
