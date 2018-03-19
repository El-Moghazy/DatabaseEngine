import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;



public class BrinLayer implements Serializable {

	private String indexPath, BrinLayerPath , denseLayerPath;
	private String indexkey;
	private int noPages;
	
	public BrinLayer(String indexPath,String indexkey) throws IOException, ClassNotFoundException, DBAppException{
		this.indexkey=indexkey;
		this.indexPath=indexPath;
		BrinLayerPath=indexPath+"BrinLayer"+'/';
		denseLayerPath = indexkey + "DenseLayer/";
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
			for (int i = 0; i < ddense.noPages; i++) 
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
				if(curPage.isFull()){
					curPage.savePage();
					curPage = createPage();
				}
				
				curPage.insert(tuple, false);
			}
			curPage.savePage();
			
			
			
		}

	    private Page createPage() throws IOException {
	    	Page page = new Page(BrinLayerPath+ indexkey +  "brin_" + (++noPages) + ".class");
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
	    
		public ArrayList<Integer> search(Object min,Object max,boolean minEq,boolean maxEq) throws FileNotFoundException, IOException, ClassNotFoundException {
			ArrayList<Integer> pages= new ArrayList<>();
			for(int i=0;i<noPages;i++){
				String name=BrinLayerPath+ indexkey +  "brin_" + i + ".class";
				ObjectInputStream ois2 = new ObjectInputStream(new FileInputStream(name));
				Page brin = (Page) ois2.readObject();
				for(Tuple t:brin.getTuples()){
					if(compare(max, t.getValues()[0])>=0 && compare(min, t.getValues()[1])<=0)
						pages.add((Integer)t.getValues()[2]);
						if(compare(min, t.getValues()[1])>0)
							break;
					}
					ois2.close();
			}
					return pages;
		}

		public int compare(Object x,Object y){
			switch (y.getClass().getName().toLowerCase()) {
            case "java.lang.integer":
                return ((Integer) x).compareTo(((Integer) y));
            case "java.lang.string":
                return ((String) x).compareTo(((String) y));
            case "java.lang.double":
                return ((Double) x).compareTo(((Double) y));
            case "java.lang.boolean":
                return ((Boolean) x).compareTo(((Boolean) y));
            case "java.util.date":
                return ((Date) x).compareTo(((Date) y));
        }
        return 0;
			
		}
		
		public void refresh(int densePageNumber,int maxDensePageNumber) throws FileNotFoundException, IOException, ClassNotFoundException
		{
			int tuplesPerPage = (new Configuration()).getMaximumSize();
			int brinPageNumber = densePageNumber /  tuplesPerPage;
			int tuplePointer = densePageNumber % tuplesPerPage;
			
			while(densePageNumber<=maxDensePageNumber)
			{
				
				File file = new File(indexPath + indexkey+"index_"+ brinPageNumber +".class");
				Page brinPage = null;
				if (file.exists()) {
					ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
					brinPage= (Page) ois.readObject();
					ois.close();
				}
				if(brinPage==null)
					brinPage=createPage();
				
				
				file = new File( denseLayerPath+ indexkey+"dense_"+ densePageNumber +".class");
				Page densePage = null;
				if (file.exists()) {
					ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
					densePage= (Page) ois.readObject();
					ois.close();
				}
				if(densePage==null)return;
				
				
				Object[] values = new Object[3];
				values[0] =  densePage.getTuples().get(0).get()[0];
				values[1] = densePage.getTuples().get(densePage.getTupleCount()-1).get()[0];
				values[2] = densePageNumber;
				
				String[] types = new String[3];
				types[0] = densePage.getTuples().get(0).getTypes()[0];
				types[1] = densePage.getTuples().get(densePage.getTupleCount()-1).getTypes()[0];
				// TODO Fix types[2]
				types[2] = "java.lang.integer";
				
				String[] colName = new String[3];
				colName[0] = densePage.getTuples().get(0).colName[0];
				colName[1] = densePage.getTuples().get(0).colName[0];
				colName[2] = "page.number";
				
				Tuple tuple=new Tuple(values, types, colName, 0);
				brinPage.getTuples().set(tuplePointer++, tuple);
				
				densePageNumber++;
				if(tuplePointer >= tuplesPerPage)
					brinPageNumber++;
			}
		}
}
