import java.io.*;
import java.util.*;

public class DBAppTest {

	@SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {

		String strTableName = "Student";

		DBApp ourDB = new DBApp(strTableName, 100);

		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		ourDB.createTable( strTableName, "id", htblColNameType );
		//		createBRINIndex( strTableName, "gpa" );
		Hashtable htblColNameValue = new Hashtable( );
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) ); 
		ourDB.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 453455 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 0.95 ) ); 
		ourDB.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5674567 ));
		htblColNameValue.put("name", new String("Dalia Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 1.25 ) ); 
		ourDB.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( ); 
		htblColNameValue.put("id", new Integer( 23498 )); 
		htblColNameValue.put("name", new String("John Noor"));
		htblColNameValue.put("gpa", new Double( 1.5 ) ); 
		ourDB.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( ); 
		htblColNameValue.put("id", new Integer( 78452 ));
		htblColNameValue.put("name", new String("Zaky Noor"));
		htblColNameValue.put("gpa", new Double( 0.88 ) ); 
		ourDB.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( ); 

	}

}
