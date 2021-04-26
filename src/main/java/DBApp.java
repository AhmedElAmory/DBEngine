import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

public class DBApp implements DBAppInterface {
	
	//Constructor
	public DBApp() {
		init();
	}
	
	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {
		
		if(!new File("src\\main\\resources\\data").exists()) {
			//Creating a File object
			File file = new File("src\\main\\resources\\data");
			//Creating the directory
			boolean bool = file.mkdir();
	   
		}
		
		
	}
	
	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	// htblColNameMin and htblColNameMax for passing minimum and maximum values
	// for data in the column. Key is the name of the column
	
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		
		checkCreateTableExceptions(strTableName,strClusteringKeyColumn,htblColNameType);
		try {
			//Reading the metadata file
			FileWriter csvWriter = new FileWriter("src\\main\\resources\\metadata.csv",true);
			//Looping over the Hashtable
			Set<String> keys = htblColNameType.keySet();
			for(String key: keys) {
				boolean cluster=false;
				if(strClusteringKeyColumn.equals(key)) {
					cluster=true;
				}
				csvWriter.append("\n"+strTableName+","+key+","+htblColNameType.get(key)+","+cluster+","+false+","+htblColNameMin.get(key)+","+htblColNameMax.get(key));
			}
			csvWriter.flush();
			csvWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//Delete class table and perform the csv stuff here ...
		//make sure to check for datatypes that they are from the 4 specified
	}


	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			int columnsFound=0;
			boolean foundCluster=false;
			while (current != null) {
				String arr[]=current.split(",");
				//Check if same Table
				if(arr[0].equals(strTableName)) {
					//Check if the column is being inserted in the hashtable
					if(htblColNameValue.containsKey(arr[1])) {
						try {
							//Check if the class of the object in the hashtable is an instance of 
							//the column class
							if(!Class.forName("arr[2]").isInstance(htblColNameValue.get(arr[1]))) {
								throw new DBAppException();
							}
							columnsFound++;
							
							if(arr[3].equals("True")) {
								foundCluster=true;
							}
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
					}
				}
			}
			
			if(!foundCluster|columnsFound!=htblColNameValue.size()) {
				throw new DBAppException();
			}
			
			//Insertion sort into existing pages or create new page if no pages exit
			//
			//
			//
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	
	
	
	
	
	public void abata(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			boolean tableNameflag=false;
			int columnsFound=0;
			while (current != null) {
				
				String arr[]=current.split(",");
				
				if(htblColNameValue.containsKey(arr[1])){
					Object value = htblColNameValue.get(arr[1]);
				}
				
				
				if(arr[0].equals(strTableName)&&arr[3].equals("True")) {
					if(!htblColNameValue.containsKey(arr[1])) {
						throw new DBAppException();
					}
					columnsFound++;
				}else if(arr[0].equals(strTableName)&&htblColNameValue.containsKey(arr[1])&&arr[2].equals(htblColNameValue.get(arr[1]))) {
					columnsFound++;
				}
			}
			
			if(columnsFound!=htblColNameValue.size()) {
				throw new DBAppException();
			}
			
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		File dir = new File("src\\main\\resources\\data");
		 File[] directoryListing = dir.listFiles();
		 Boolean found=false;
		 if (directoryListing != null) {
		   for (File page : directoryListing) {
		     String name=page.getName();
		     
		   }
		 }
		
	}
	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the rows to update.
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue entries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

	}

	
	// following method creates one index – either multidimensional
	// or single dimension depending on the count of column names passed.
	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		
	}
	
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		return null;
	}
	
	
	
	//Supplement methods
	
	
	//This method check if the inserted table already exists.
	public void checkTableExists(String tableName) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			while (current != null) {
				String arr[]=current.split(",");
				if(arr[0].equals(tableName)) {
					throw new DBAppException();
				}
				current=br.readLine();
			}
			br.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	public void checkDataTypeAndPrimaryExists(Hashtable<String, String> htblColNameType,String strClusteringKeyColumn) throws DBAppException {
		
		boolean clusterFound=false;
		Set<String> keys = htblColNameType.keySet();
		for(String key: keys) {
			//Check if primary key exists
			if(key.equals(strClusteringKeyColumn)) {
				clusterFound=true;
			}
			String dataType=htblColNameType.get(key);
			if(!(dataType.equals("java.lang.Integer")|
					dataType.equals("java.lang.String")|
					dataType.equals("java.lang.Double")|
					dataType.equals("java.util.Date"))) {
				throw new DBAppException();
			}
		}
		if(!clusterFound) {
			throw new DBAppException();
		}
	}
	
	public void checkCreateTableExceptions(String strTableName,String strClusteringKeyColumn,Hashtable<String, String> htblColNameType) throws DBAppException {
		//check if table already exists
		checkTableExists(strTableName);
		//check inserted dataTypes
		checkDataTypeAndPrimaryExists(htblColNameType,strClusteringKeyColumn);
	}

}
