import java.io.*;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.*;

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
//			boolean foundPage=false;
//			for (File page : directoryListing) {
//				String tableName=getFileTableName(page.getName());
//				if(tableName.equals(strTableName)) {
//					foundPage=true;
//				}
//			}

			int numberOfpages;
			numberOfpages = countNumberOfPagesWithoutOverflow(strTableName);

			if(numberOfpages==0) {
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
				String primaryKey="";
				String primarykeyType="";
				try {
					BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
					String current = br.readLine();
					// check if column names in hashtable exist in metadata
					while(current!=null) {
						String arr[]=current.split(",");
						//check if metadata row has same table name as input
						if(arr[0].equals(strTableName)) {
							//check if hashtable contains same column name as metadata
							if(arr[3].equals("true")) {
								primaryKey=arr[1];
								primarykeyType=arr[2];
							}
						}
					}
				}
				catch (IOException e) {
					e.printStackTrace();
				}
				Object primarykeyvalue=htblColNameValue.get(primaryKey);



				
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

	// comparing objects
	public int compare(Object obj1, Object obj2, String primarykeyType){

		if(primarykeyType.equals("java.lang.Double")){
			if(((double)obj1)>((double)obj2))
				return 1;
			else if(((double)obj1)<((double)obj2))
				return -1;
			else
				return 0;
		}
		else if(primarykeyType.equals("java.lang.Integer")){
			if(((int)obj1)>((int)obj2))
				return 1;
			else if(((int)obj1)<((int)obj2))
				return -1;
			else
				return 0;
		}
		else if(primarykeyType.equals("java.util.Date")){
			return 	((Date)obj1).compareTo((Date)obj2);
		}
		else if(primarykeyType.equals("java.lang.String")){
			return ((String)obj1).compareTo((String)obj2);
		}
		return -100;
	}

	//searching for the overflow page needed to store the new input
	public String binarySearchOnOverflowPages(String TableName, int TotalNumberOfOverflowPages, String PrimaryKey,
											  Object PrimaryKeyValue, String primarykeyType, int page) throws DBAppException{
		int startvalue = 0;
		int endvalue = TotalNumberOfOverflowPages;
		int mid;
		while (startvalue<=endvalue) {

			mid = (startvalue + endvalue) / 2;
			//here after getting the middle page of the overflow pagesof this specific page we get its path
			String path = "src\\main\\resources\\data\\" + TableName + "[" + page + "](" + mid + ").class";
			//getting the this over flow page to check if its in the range of this overflow page
			Vector<Hashtable<String,Object>> v = null;
			try {
				FileInputStream fileIn = new FileInputStream(path);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				v = (Vector) in.readObject();
				in.close();
				fileIn.close();
			} catch (IOException i) {
				i.printStackTrace();
			} catch (ClassNotFoundException c) {
				c.printStackTrace();
			}
			//max val of this overflow page
			Object maxValue = v.get(v.size()-1).get(PrimaryKey);
			//min val of this overflow page
			Object minValue = v.get(0).get(PrimaryKey);

			//comparing the primary key value to the min value
			int comp = compare(PrimaryKeyValue,minValue,primarykeyType);
			if(comp==0){
				throw new DBAppException("Primary key already exists!");
			}
			//if the primary value <min val we take the half before the mid
			else if(comp<0){
				endvalue=mid-1;
			}else{

				// else we compare it to the max val
				int comp2 = compare(PrimaryKeyValue,maxValue,primarykeyType);
				if(comp2==0){
					throw new DBAppException("Primary key already exists!");
				}
				// if it is less than the max val then its in in this over flow page
				else if(comp2<0){
					return TableName + "[" + page + "](" + mid + ").class";
				}
				//else we take the half after the mid
				else{
					startvalue=mid+1;
				}
			}
		}
		return "";
	}
	public String binarySearchOnPages(String TableName, int TotalNumberOfPages, String PrimaryKey, Object PrimaryKeyValue, String primarykeyType) throws DBAppException {

		int startvalue = 1;
		int endvalue = TotalNumberOfPages;
		int mid;



		//checking if the value of the input less than the min val in the table an rreturning the first page
		String path3 = "src\\main\\resources\\data\\" + TableName + "[1](0).class";

		Vector<Hashtable<String,Object>> v5 = null;
		try {
			FileInputStream fileIn = new FileInputStream(path3);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v5 = (Vector) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			c.printStackTrace();
		}
		Object minValue3 = v5.get(0).get(PrimaryKey);
		int comp5 = compare(PrimaryKeyValue,minValue3,primarykeyType);
		if(comp5<0){
			return TableName + "[1](0).class";
		}

		// binary search for the page

		while (startvalue<=endvalue){

			mid = (startvalue + endvalue)/2;
			String path = "src\\main\\resources\\data\\" + TableName + "[" + mid + "](0).class";

			Vector<Hashtable<String,Object>> v = null;
			try {
				FileInputStream fileIn = new FileInputStream(path);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				v = (Vector) in.readObject();
				in.close();
				fileIn.close();
			} catch (IOException i) {
				i.printStackTrace();
			} catch (ClassNotFoundException c) {
				c.printStackTrace();
			}
			Object maxValue = null;
			// getting the min val of the page
			Object minValue = v.get(0).get(PrimaryKey);

			//getting the number of overflow pages of this page
			int numberofoverflows = countNumberOfPagesWithOverflow(TableName,mid);

			//if its a zero then it doesnot have overflows then its max is in this page in the end
			if (numberofoverflows==0){
				maxValue = v.get(v.size()-1).get(PrimaryKey);
			}else{
				//else we get the last overflow page and get its last vlue to be the max val
				Vector<Hashtable<String,Object>> v2 = null;
				String path2 = "src\\main\\resources\\data\\" + TableName + "[" + mid + "](" + numberofoverflows + ").class";
				try {
					FileInputStream fileIn = new FileInputStream(path2);
					ObjectInputStream in = new ObjectInputStream(fileIn);
					v2 = (Vector) in.readObject();
					in.close();
					fileIn.close();
				} catch (IOException i) {
					i.printStackTrace();
				} catch (ClassNotFoundException c) {
					c.printStackTrace();
				}
				maxValue= v2.get(v2.size()-1).get(PrimaryKey);
			}
			// wecompare  the primary val to the min val
			int comp = compare(PrimaryKeyValue,minValue,primarykeyType);
			if(comp==0){
				throw new DBAppException("The primary key already exists!");
			}
			//if it is less than the min val then we take the part before the mid val
			else if(comp<0){
				endvalue = mid-1;
			}
			//else if it is greater than the min
			else{
				// compare the primary val to the max val
				int comp2 = compare(PrimaryKeyValue,maxValue,primarykeyType);
				if(comp2==0){
					throw new DBAppException("The primary key already exists!");
				}
				//if it is grater than the max val then we take the part after the mid
				else if(comp2>0){
					startvalue=mid +1;

				}//if the primary val less than the max then its in this page mid or one of its over flow
				 else{
				 	//if there is no overflows we return this page
					if(numberofoverflows==0){
						return TableName + "[" + mid + "](0).class";
					}// else we binary search on the page and its overflows to find its correct page
					else {
						String overflowres = binarySearchOnOverflowPages(TableName, numberofoverflows,
								PrimaryKey, PrimaryKeyValue, primarykeyType, mid);
						return overflowres;
					}
				}
			}
		}

		return "";
	}


	//count the pages for a specific table
	public int countNumberOfPagesWithoutOverflow(String Tablename){

		File dir = new File("src\\main\\resources\\data");
		File[] directoryListing = dir.listFiles();
		int counter =0;
		if (directoryListing != null) {
			for (File page : directoryListing) {
				String tableName=getFileTableName(page.getName());
				int overflowno = getFileOverflowNumber(page.getName());
				if(tableName.equals(Tablename) && overflowno==0) {
					counter++;
				}
				}
			}
		return counter;
	}

	//count the overflow pages for a specific page
	public int countNumberOfPagesWithOverflow(String Tablename, int Number){

		File dir = new File("src\\main\\resources\\data");
		File[] directoryListing = dir.listFiles();
		int counter =0;
		if (directoryListing != null) {
			for (File page : directoryListing) {
				String tableName=getFileTableName(page.getName());
				int overflowno = getFileOverflowNumber(page.getName());
				int pageNumber = getFilePageNumber(page.getName());

				if(tableName.equals(Tablename) && pageNumber==Number && overflowno!=0) {
					counter++;
				}
			}
		}
		return counter;
	}




}
