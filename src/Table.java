import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import com.sun.javafx.scene.paint.GradientUtils.Point;

public class Table implements Serializable {

    private static final long serialVersionUID = 1L;
    private ArrayList<Object> PrimaryKeyCheck;
    private transient ArrayList<BrinIndex> indexList;

    public ArrayList<Object> getPrimaryKeyCheck() {
        return PrimaryKeyCheck;
    }

    private Hashtable<String, String> htblColNameType;
    private String tableName, path, strClusteringKeyColumn;
    private int curPageIndex, maxPageSize, numOfCols,pagetmp;

    public int getCurPageIndex() {
        return curPageIndex;
    }

    public Table(String tableName, String path, Hashtable<String, String> htblColNameType,
                 String strClusteringKeyColumn) throws DBAppException, IOException {
        Configuration config = new Configuration();
        PrimaryKeyCheck = new ArrayList();
        this.maxPageSize = config.getMaximumSize();
        this.tableName = tableName;
        this.path = path + tableName + '/';
        this.htblColNameType = htblColNameType;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        numOfCols = htblColNameType.size();
        curPageIndex = -1;

        createTableDirectory();
        createPage();
        saveTable();

    }

    private Tuple makeTuple(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        checkInsertedColumns(htblColNameValue);

        Object value = htblColNameValue.get(strClusteringKeyColumn);
        if (value == null)

            throw new DBAppException("Clustering key is not allowed to be null");

        Object[] values = new Object[numOfCols + 1];
        String[] types = new String[numOfCols + 1];
        String[] names=new String[numOfCols + 1];
        Set<String> columns = htblColNameValue.keySet();
        int i = 0;
        int keyIndex = numOfCols;
        for (String column : columns) {
            names[i]=column;
            if (column.equals(strClusteringKeyColumn))
                keyIndex = i;

            types[i] = htblColNameType.get(column);
            values[i] = htblColNameValue.get(column);
            i++;
        }
        Date d = Calendar.getInstance().getTime();
        values[numOfCols] = d;
        names[numOfCols]= "insert time";
        types[numOfCols] = "java.util.date";
        return new Tuple(values, types, names,keyIndex);
    }

    public boolean insert(Hashtable<String, Object> htblColNameValue)
            throws DBAppException, ClassNotFoundException, IOException {
        Tuple t = makeTuple(htblColNameValue);
        Object value = htblColNameValue.get(strClusteringKeyColumn);
        if (PrimaryKeyCheck.contains(value))
            throw new DBAppException("Insertion in table failed. PrimaryKey value already exist in the table");
       int page= insertTuple(t);
        PrimaryKeyCheck.add(value);
        saveTable();
        rollBack();
        saveTable();
        return true;

    }

    public boolean delete(Hashtable<String, Object> htblColNameValue)
            throws DBAppException, ClassNotFoundException, IOException {
        Tuple t = makeTuple(htblColNameValue);
        Object value = htblColNameValue.get(strClusteringKeyColumn);
        deleteTuple(t);
        if (PrimaryKeyCheck.contains(value))
            PrimaryKeyCheck.remove(value);
        saveTable();
        rollBack();
        saveTable();
        return true;
    }

    public boolean update(String strKey, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {
        Tuple t = makeTuple(htblColNameValue);
        Object value = htblColNameValue.get(strClusteringKeyColumn);
        Object oldKey = fromStringToObject(strKey, htblColNameType.get(strClusteringKeyColumn));
        if (!value.toString().equals(oldKey.toString()))
            throw new DBAppException("update in table failed. PrimaryKeys are not the same");
       Tuple old= updateTuple(oldKey, t);
        PrimaryKeyCheck.add(value);
        if (PrimaryKeyCheck.contains(oldKey))
            PrimaryKeyCheck.remove(oldKey);
        saveTable();
        rollBack();
        saveTable();
        return true;
    }

    private Tuple updateTuple(Object oldKey, Tuple tuple)
            throws FileNotFoundException, IOException, ClassNotFoundException, DBAppException {
        for (int i = 0; i <= curPageIndex; i++) {
            File file = new File(path + tableName + "_" + i + ".class");
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
            Page curPage = (Page) ois.readObject();

            if (curPage.exist(oldKey)) {
                Tuple t = curPage.getThisTuple(oldKey);
                curPage.delete(t);
                curPage.insert(tuple,true);
                ois.close();
                  pagetmp = i;

                return t;
            }
            ois.close();
        }
        throw new DBAppException("This key does not exist in the table");

    }

    public String getStrClusteringKeyColumn() {
        return strClusteringKeyColumn;
    }

    private void deleteTuple(Tuple tuple) throws IOException, DBAppException, ClassNotFoundException {
        for (int i = 0; i <= curPageIndex; i++) {
            File file = new File(path + tableName + "_" + i + ".class");
//            System.out.println(file.exists());
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
            ObjectInputStream ois2 = null;
            Page curPage = (Page) ois.readObject();

            if (curPage.getTuples().contains((tuple))) {
                curPage.delete(tuple);
                if (curPage.getTuples().size() == 0) {
                    ois.close();
                    System.out.println("Delete: " + file.delete());
                    curPageIndex--;
                    return;
                } else {
                    int j = i + 1;
                    File file2 = new File(path + tableName + "_" + j + ".class");
                    while (file2.exists()) {
                        ois2 = new ObjectInputStream(new FileInputStream(file2));
                        Page nxtPage = (Page) ois2.readObject();
                        Tuple tuple1 = nxtPage.getTuples().get(0);
                        curPage.insert(tuple1,true);
                        nxtPage.delete(tuple1);
//                      deleteTuple(tuple1);
                        if (nxtPage.getTupleCount() == 0)
                            file2.delete();
                        j++;
                        curPage = nxtPage;
                        file2 = new File(path + tableName + "_" + j + ".class");

                    }
                    if (ois2 != null)
                        ois2.close();
                    ois.close();
                    return;
                }
            }
            ois.close();
        }
        throw new DBAppException("This row does not exist in the table");
    }

    public int insertTuple(Tuple tuple) throws IOException, DBAppException, ClassNotFoundException {

        for (int i = 0; i <= curPageIndex; i++) {
            File file = new File(path + tableName + "_" + i + ".class");
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
            Page curPage = (Page) ois.readObject();
            if (!curPage.isFull()) {
                curPage.insert(tuple,true);
                ois.close();
                return i;
            } else {
                if (tuple.compareTo(curPage.getTuples().get(curPage.getTupleCount() - 1)) < 0) {
                    Tuple t = curPage.getTuples().get(curPage.getTupleCount() - 1);
                    curPage.setTupleCount(curPage.getTupleCount() - 1);
                    curPage.getTuples().remove(t);
                    curPage.insert(tuple,true);
                    ois.close();
                    insertTuple(t);

                    return i;
                }
            }

            ois.close();
        }
        Page curPage = createPage();
        curPage.insert(tuple,true);
        return curPageIndex;
    }

    private void checkInsertedColumns(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        for (Entry<String, Object> entry : htblColNameValue.entrySet()) {
            String colName = entry.getKey();
            if (!htblColNameType.containsKey(colName))
                throw new DBAppException("Column : " + colName + " doesn't exist");
            if (checkValueType(entry.getValue(), htblColNameType.get(colName)))
                throw new DBAppException("Type mismatch on column : " + colName);
        }
    }

    public boolean checkValueType(Object value, String type) {

        switch (type.toLowerCase()) {
            case "java.lang.integer":
                if (!(value instanceof Integer))
                    return false;
            case "java.lang.string":
                if (!(value instanceof String))
                    return false;
            case "java.lang.double":
                if (!(value instanceof Double))
                    return false;
            case "java.lang.boolean":
                if (!(value instanceof Boolean))
                    return false;
            case "java.util.date":
                if (!(value instanceof Date))
                    return false;
            default:
                return true;
        }
    }

    private Object fromStringToObject(String strKey, String type) throws ParseException, DBAppException {
        Object key = new Object();
        try {
            switch (type.toLowerCase()) {
                case "java.lang.integer":
                    key = Integer.parseInt(strKey);
                    break;
                case "java.lang.string":
                    key = strKey;
                    break;
                case "java.lang.double":
                    key = Double.parseDouble(strKey);
                    break;
                case "java.lang.boolean":
                    key = Boolean.parseBoolean(strKey);
                    break;
                case "java.util.date":
                    SimpleDateFormat format = new SimpleDateFormat("EEE MMM DD HH:mm:ss zzz yyyy");
                    key = format.parse(strKey);
                    break;
            }
            return key;
        } catch (Exception e) {
            throw new DBAppException("this Key have the wrong syntax");
        }
    }

    private void createTableDirectory() {
        File table = new File(path);
        table.mkdir();
    }

    private Page createPage() throws IOException {
        Page page = new Page(path + tableName + "_" + (++curPageIndex) + ".class");
        saveTable();
        return page;
    }

    private void saveTable() throws IOException {
        File table = new File(path + tableName + ".class");
        if (!table.exists())
            table.createNewFile();
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(table));
        oos.writeObject(this);
        oos.close();
    }

    public BrinIndex getIndex(String strColName) throws FileNotFoundException, IOException, ClassNotFoundException
    {
    	File file = new File(path+ strColName + ".class");
		if (file.exists()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
			BrinIndex Index = (BrinIndex) ois.readObject();
			ois.close();
			return Index;
		}

    	return null;
    }

    public void createBRINIndex(String strColName) throws DBAppException, IOException, ClassNotFoundException
	{
    	if(!this.htblColNameType.containsKey(strColName))
    		throw new DBAppException("this column does not exist");
    	Hashtable<String, String> htblColNameType = new Hashtable<>();
    	htblColNameType.put(strColName, this.htblColNameType.get(strColName));
    	htblColNameType.put(strClusteringKeyColumn, this.htblColNameType.get(strClusteringKeyColumn));

		new BrinIndex(path, htblColNameType,strColName,strClusteringKeyColumn,tableName);
	}


	public int getMaxPageSize() {return maxPageSize;}

	/*
	 * Returns all of the BRIN indices created on this table
	 * */
    public ArrayList<BrinIndex> fetchBRINindices() throws FileNotFoundException, IOException, ClassNotFoundException
    {
    	File dir = new File(path);
    	ArrayList<BrinIndex> list = new ArrayList<BrinIndex>();
    	for(File folder : dir.listFiles())
    	{
    		if(folder.isDirectory())
    		{
    			File file = new File(path+folder.getName()+"/"+folder.getName()+".class");
    			if (file.exists())
    			{
    				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
    				BrinIndex index = (BrinIndex) ois.readObject();
    				ois.close();
    				list.add(index);
    			}
    		}
    	}
    	return indexList = list;
    }

    public BrinIndex fetchBRINindex(String strColName) throws FileNotFoundException, IOException, ClassNotFoundException
    {
    	File dir = new File(path);
    	for(File folder : dir.listFiles())
    	{
    		if(folder.isDirectory())
    		{
    			File file = new File(path+folder.getName()+"/"+folder.getName()+".class");
    			if (file.exists())
    			{
    				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
    				BrinIndex index = (BrinIndex) ois.readObject();
    				ois.close();
    				return index;
    			}
    		}
    	}
    	return null;
    }
    public Iterator<Tuple> search(String strColumnName, Object[] objarrValues, String[] strarrOperators)
            throws FileNotFoundException, ClassNotFoundException, IOException {
        BrinIndex index = fetchBRINindex(strColumnName);

        Object min;
        Object max;
        switch (htblColNameType.get(strColumnName).toLowerCase()) {
        case "java.lang.integer":
            min = Integer.MIN_VALUE;
            max = Integer.MAX_VALUE;
            break;
        case "java.lang.string":
            min = "";
            max = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ";
            break;
        case "java.lang.double":
            min = Double.MIN_VALUE;
            max = Double.MAX_VALUE;
            break;

        case "java.util.date":
            min = new Date(Long.MIN_VALUE);
            max = new Date(Long.MAX_VALUE);
            break;
        default:
            min = Integer.MIN_VALUE;
            max = Integer.MAX_VALUE;
        }

        boolean mineq = false;
        boolean maxeq = false;
        for (int i = 0; i < objarrValues.length; i++) {
            switch (strarrOperators[i]) {
            case ">=":
                min = min(min, objarrValues[i]);
                mineq = true;
                break;
            case ">":
                min = min(min, objarrValues[i]);
                break;
            case "<=":
                max = max(max, objarrValues[i]);
                maxeq = true;
                break;
            case "<":
                max = max(max, objarrValues[i]);
                break;
            default:
                break;
            }
        }

        if (index == null) {

            ArrayList<Tuple> tabletubles = new ArrayList<Tuple>();

            for (int i = 0; i <= this.getCurPageIndex(); i++) {
                File table = new File(
                        "databases/" + tableName + "/" + tableName + "/" + tableName + "_" + i + ".class");
                System.out.println("databases/" + tableName + "/" + tableName + "/" + tableName + "_" + i + ".class");
                InputStream file = new FileInputStream(table);
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer);
                try {

                    Page p = (Page) input.readObject();

                    ArrayList<Tuple> t = p.getTuples();

                    for (Tuple tt : t) {
                        if (tt != null) {
                            int myindex = tt.getIndex(strColumnName);
                            Object o = tt.getValues()[myindex];
                             if(compare(o,min)>=0 && compare(o,max)<=0){
                                if(!((compare(o,min)==0 && !mineq )|| (compare(o,max)==0 && !maxeq)))
                                    tabletubles.add(tt);
                            }

                        }
                    }

                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                input.close();
            }

            return tabletubles.iterator();

        } else {

            Iterator<Tuple> indextuples = index.search(min, max, mineq, maxeq);
            ArrayList<Tuple> tabletubles = new ArrayList<Tuple>();
            while (indextuples.hasNext()) {
                Tuple t = indextuples.next();
//                tabletubles.add(binarySearch(t.get()[t.getKey()]));
                File file = new File(path + tableName + "_" + t.getValues()[2] + ".class");
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
                Page curPage = (Page) ois.readObject();
                tabletubles.add(curPage.getThisTuple(t.getValues()[1]));
            }
            return tabletubles.iterator();

        }
    }

	private Object max(Object max, Object object) {

        String type =object.getClass().toString();

        if (type.contains("Integer")) {
            return Math.max((Integer)max,(Integer) object);
        } else if (type.contains("Double")) {
        	 return Math.max((Double)max,(Double) object);

        } else if (type.contains("String")) {
        	if(((String)max).compareTo((String)object)==1)
        		return max;
        	else
        		return object;
        } else if (type.contains("Date")) {
        	if(((Date)max).compareTo((Date)object)==1)
    			return max;
    		else
    			return object;

        }

		return null;
	}

	private Object min(Object min, Object object) {
		   String type =object.getClass().toString();

	        if (type.contains("Integer")) {
	            return Math.min((Integer)min,(Integer) object);
	        } else if (type.contains("Double")) {
	        	 return Math.min((Double) min,(Double) object);

	        } else if (type.contains("String")) {
	        	if(((String)min).compareTo((String)object)==-1)
	        		return min;
	        	else
	        		return object;
	        } else if (type.contains("Date")) {
	        	if(((Date)min).compareTo((Date)object)==-1)
	    			return min;
	    		else
	    			return object;

	        }

			return null;
	}

	private Tuple binarySearch(Object key) throws FileNotFoundException, ClassNotFoundException, IOException
	{
		int lo = 0 , hi = curPageIndex , index = -1;

		while(lo <= hi)
		{
			int mid = (hi+lo)/2;
			Page curPage = loadPage(mid);

			Tuple firstTuple = curPage.getTuples().get(0);
			Tuple lastTuple = curPage.getTuples().get(curPage.getTupleCount());

			Object p1 = firstTuple.get()[firstTuple.getKey()];
			Object p2 = lastTuple.get()[lastTuple.getKey()];

			if(compare(key, p1)>=0 && compare(key, p2)<=0)
			{
				index = mid;
				break;
			}
			if(compare(key, p2)>0)
			{
				lo = mid+1;
			}
			else
			{
				hi = mid-1;
			}
		}

		if(index==-1)
			return null;
		return loadPage(index).getThisTuple(key);
	}

	public Page loadPage(int pageNumber) throws FileNotFoundException, IOException, ClassNotFoundException
	{
		File file = new File(path + tableName+"_"+ pageNumber +".class");
		Page page = null;
		if (file.exists()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
			page= (Page) ois.readObject();
			ois.close();
		}
		return page;
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
	
	public void rollBack() throws IOException, ClassNotFoundException, DBAppException
	{
		fetchBRINindices();
        ArrayList<String> indexedColNames = new ArrayList<String>();
        
        for (BrinIndex index : indexList)
        {
//        	index.insertTuple(t,page);
        	indexedColNames.add(index.getIndexColName());
//        	createBRINIndex(index.getIndexColName());
        	index.drop();
        }
        indexList = new ArrayList<BrinIndex>();
        for(String col : indexedColNames)
        	createBRINIndex(col);

	}
}
