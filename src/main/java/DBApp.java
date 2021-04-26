import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
		

	}


	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
	
		//check if columns in hashtable exist in metadata and are of correct datatypes
//////////check date-time input constraintss///////////////////////////////////////////////////////////
		checkInsertInputConstraints(strTableName,htblColNameValue);
		
		File dir = new File("src\\main\\resources\\data");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			boolean foundPage=false;
			for (File page : directoryListing) {
				String tableName=getFileTableName(page.getName());
				if(tableName.equals(strTableName)) {
					foundPage=true;
				}
			}
			if(!foundPage) {
				Vector<Hashtable<String,Object>> newPage =new Vector();
				newPage.add(htblColNameValue);
				try {
			         FileOutputStream fileOut =
			         new FileOutputStream("src\\main\\resources\\data\\"+strTableName+"[1](0)"+".class");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(newPage);
			         out.close();
			         fileOut.close();
			         System.out.println("src\\main\\resources\\data\\"+strTableName+"[1](0)"+".class");
			      } catch (IOException i) {
			         i.printStackTrace();
			      }
				
				
			}else {
				
			}
			
			
		}
		
		 
		 //for loop over file names get min page number and max page number
		 // get to the middle page and get min primary key and max primary key ....(from it and it's overflows)
		 //if less than look left.. if greater than look right.. if between binary search over the overflows
		 //and when reaching the required page binary search over the primary key...
		 
			
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
	public boolean checkTableExists(String tableName)  {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			while (current != null) {
				String arr[]=current.split(",");
				if(arr[0].equals(tableName)) {
					return true;
				}
				current=br.readLine();
			}
			
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
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
			if(!(dataType.equals("java.lang.Integer")||
					dataType.equals("java.lang.String")||
					dataType.equals("java.lang.Double")||
					dataType.equals("java.util.Date"))) {
				throw new DBAppException("Wrong datatypes!");
			}
		}
		if(!clusterFound) {
			throw new DBAppException("Primary key not found!");
		}
	}
	
	public void checkCreateTableExceptions(String strTableName,String strClusteringKeyColumn,Hashtable<String, String> htblColNameType) throws DBAppException {
		//check if table already exists
		if(checkTableExists(strTableName)) {
			throw new DBAppException("Table already exists!");
		}
		//check inserted dataTypes
		checkDataTypeAndPrimaryExists(htblColNameType,strClusteringKeyColumn);
	}
	
	public void checkInsertInputConstraints(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
		if(!checkTableExists(strTableName)) {
			throw new DBAppException("Table does not exist!");
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			// check if column names in hashtable exist in metadata
			int countCorrectColumns=0;
			boolean primaryKeyFound=false;
			while(current!=null) {
				String arr[]=current.split(",");
				//check if metadata row has same table name as input
				if(arr[0].equals(strTableName)) {
					//check if hashtable contains same column name as metadata
					if(htblColNameValue.containsKey(arr[1])) {
						//check if this is primary key
						if(arr[3].equals("true")) {
							primaryKeyFound=true;
						}
						//check if metadata row has same datatype of input value
						try {
							if(!Class.forName(arr[2]).isInstance(htblColNameValue.get(arr[1]))) {
								throw new DBAppException("Wrong datatype in column"+arr[1]+" !");
							}
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						countCorrectColumns++;
					}
				}
				current=br.readLine();
			}
			
			br.close();
			if(!(countCorrectColumns==htblColNameValue.size())) {
				throw new DBAppException("Hashtable Columns are not in metadata!");
			}
			if(!primaryKeyFound) {
				throw new DBAppException("Primary key not inserted!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	public String getFileTableName(String fileName) {
		String tableName="";
		int c=0;
		while(fileName.charAt(c)!='[') {
			tableName+=fileName.charAt(c);
			c++;
		}
		return tableName;
		
	}
	
	public int getFilePageNumber(String fileName) {
		String pageNumber="";
		int c=0;
		while(fileName.charAt(c)!='[') {
			c++;
		}
		c++;
		while(fileName.charAt(c)!=']') {
			pageNumber+=fileName.charAt(c);
			c++;
		}
		
		return Integer.parseInt(pageNumber);
	}
	
	public int getFileOverflowNumber(String fileName) {
		String overflowNumber="";
		int c=0;
		while(fileName.charAt(c)!='(') {
			c++;
		}
		c++;
		while(fileName.charAt(c)!=')') {
			overflowNumber+=fileName.charAt(c);
			c++;
		}
		
		return Integer.parseInt(overflowNumber);
	}
}
