import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;

public class Table implements Serializable {

    private static final long serialVersionUID = 1L;
    private ArrayList<Object> PrimaryKeyCheck;

    public ArrayList<Object> getPrimaryKeyCheck() {
        return PrimaryKeyCheck;
    }

    private Hashtable<String, String> htblColNameType;
    private String tableName, path, strClusteringKeyColumn;
    private int curPageIndex, maxPageSize, numOfCols;

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
        Set<String> columns = htblColNameValue.keySet();
        int i = 0;
        int keyIndex = numOfCols;
        for (String column : columns) {
            if (column.equals(strClusteringKeyColumn))
                keyIndex = i;

            types[i] = htblColNameType.get(column);
            values[i] = htblColNameValue.get(column);
            i++;
        }
        Date d = Calendar.getInstance().getTime();
        values[numOfCols] = d;
        types[numOfCols] = "java.util.date";
        return new Tuple(values, types, keyIndex);
    }

    public boolean insert(Hashtable<String, Object> htblColNameValue)
            throws DBAppException, ClassNotFoundException, IOException {
        Tuple t = makeTuple(htblColNameValue);
        Object value = htblColNameValue.get(strClusteringKeyColumn);
        if (PrimaryKeyCheck.contains(value))
            throw new DBAppException("Insertion in table failed. PrimaryKey value already exist in the table");
        insertTuple(t);
        PrimaryKeyCheck.add(value);
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
        return true;
    }

    public boolean update(String strKey, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {
        Tuple t = makeTuple(htblColNameValue);
        Object value = htblColNameValue.get(strClusteringKeyColumn);
        Object oldKey = fromStringToObject(strKey, htblColNameType.get(strClusteringKeyColumn));
        if (!value.toString().equals(oldKey.toString()))
            throw new DBAppException("update in table failed. PrimaryKeys are not the same");
        updateTuple(oldKey, t);
        PrimaryKeyCheck.add(value);
        if (PrimaryKeyCheck.contains(oldKey))
            PrimaryKeyCheck.remove(oldKey);

        saveTable();
        return true;
    }

    private void updateTuple(Object oldKey, Tuple tuple)
            throws FileNotFoundException, IOException, ClassNotFoundException, DBAppException {
        for (int i = 0; i <= curPageIndex; i++) {
            File file = new File(path + tableName + "_" + i + ".class");
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
            Page curPage = (Page) ois.readObject();

            if (curPage.exist(oldKey)) {
                Tuple t = curPage.getThisTuple(oldKey);
                curPage.delete(t);
                curPage.insert(tuple);
                ois.close();
                return;
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

            if (curPage.getTuples().contains((tuple))) 
            {
                curPage.delete(tuple);
                if (curPage.getTuples().size() == 0) 
                {
                    ois.close();
                    System.out.println("Delete: "+file.delete());
                    curPageIndex--;
                    return;
                }
                else
                {
                    int j = i + 1;
                    File file2 = new File(path + tableName + "_" + j + ".class");
                    while (file2.exists())
                    {
                        ois2 = new ObjectInputStream(new FileInputStream(file2));
                        Page nxtPage = (Page) ois2.readObject();
                        Tuple tuple1 = nxtPage.getTuples().get(0);
                        curPage.insert(tuple1);
                        nxtPage.delete(tuple1);
//                      deleteTuple(tuple1);
                        if(nxtPage.getTupleCount()==0)
                            file2.delete();
                        j++;
                        curPage = nxtPage;
                        file2 = new File(path + tableName + "_" + j + ".class");

                    }
                    if(ois2!=null)
                        ois2.close();
                    ois.close();
                    return;
                }
            }
            ois.close();
        }
        throw new DBAppException("This row does not exist in the table");
    }

    public Page insertTuple(Tuple tuple) throws IOException, DBAppException, ClassNotFoundException {

        for (int i = 0; i <= curPageIndex; i++) {
            File file = new File(path + tableName + "_" + i + ".class");
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
            Page curPage = (Page) ois.readObject();
            if (!curPage.isFull()) {
                curPage.insert(tuple);
                ois.close();
                return curPage;
            } else {
                if (tuple.compareTo(curPage.getTuples().get(curPage.getTupleCount() - 1)) < 0) {
                    Tuple t = curPage.getTuples().get(curPage.getTupleCount() - 1);
                    curPage.setTupleCount(curPage.getTupleCount() - 1);
                    curPage.getTuples().remove(t);
                    curPage.insert(tuple);
                    ois.close();
                    insertTuple(t);

                    return curPage;
                }
            }

            ois.close();
        }
        Page curPage = createPage();
        curPage.insert(tuple);
        return curPage;
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

}
