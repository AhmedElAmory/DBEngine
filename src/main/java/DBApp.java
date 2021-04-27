import java.io.*;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
//////////make sure of min max constraints and size and that they are the same as name-type hashtable
		checkInsertInputConstraints(strTableName,htblColNameValue);

		File dir = new File("src\\main\\resources\\data");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			int numberOfpages;
			numberOfpages = countNumberOfPagesWithoutOverflows(strTableName);

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
				
				String file = binarySearchOnPages(strTableName, numberOfpages, primaryKey, primarykeyvalue, primarykeyType);
				
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
	
	public void checkCreateTableExceptions(String strTableName,String strClusteringKeyColumn,Hashtable<String, String> htblColNameType) throws DBAppException {
		//check if table already exists
		if(checkTableExists(strTableName)) {
			throw new DBAppException("Table already exists!");
		}
		//check inserted dataTypes
		checkDataTypeAndPrimaryExists(htblColNameType,strClusteringKeyColumn);
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
				throw new DBAppException("Column "+key+" has an unsupported data type");
			}
		}
		if(!clusterFound) {
			throw new DBAppException("Primary key not found!");
		}
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

	public String binarySearchOnPages(String tableName, int totalNumberOfPages, 
			String primaryKey, Object primaryKeyValue, String primaryKeyType) throws DBAppException {
		int start = 1;
		int end = totalNumberOfPages;

		//checking if the value of the input is less than 
		//the minimum value in the table and returning the first page
		Vector<Hashtable<String,Object>> firstPage = readPageIntoVector(tableName+"[1](0).class");
		Object firstPageMin = firstPage.get(0).get(primaryKey);
		int compareWithFirstPageMin = compare(primaryKeyValue,firstPageMin,primaryKeyType);
		if(compareWithFirstPageMin<0){
			return tableName + "[1](0).class";
		}

		//Binary Search to get the page
		while (start<=end){
			int mid = (start + end)/2;
			Vector<Hashtable<String,Object>> currentPage = readPageIntoVector(tableName + "[" + mid + "](0).class");
			Object maxValue = null;
			
			//Getting the minimum value of the page
			Object minValue = currentPage.get(0).get(primaryKey);
			
			//Getting the number of overflow pages of this page
			int numberOfOverFlows = countNumberOfPageOverflows(tableName,mid);
			
			//If the numberOfOverFlows is zero then the maximum value is the maximum of current page
			if (numberOfOverFlows==0){
				maxValue = currentPage.get(currentPage.size()-1).get(primaryKey);
			}else{
				//if it's not 0 then we get the maximum value from the last overflow page
				Vector<Hashtable<String,Object>> lastOverflowPage = readPageIntoVector(tableName + "[" + mid + "](" + numberOfOverFlows + ").class");
				maxValue= lastOverflowPage.get(lastOverflowPage.size()-1).get(primaryKey);
			}
			// we compare the primary value to the minimum value
			int compareToMinValue = compare(primaryKeyValue,minValue,primaryKeyType);
			if(compareToMinValue==0){
				throw new DBAppException("The primary key already exists!");
			}
			else if(compareToMinValue<0){
				//if it is less than the minimum value then we take the part before the current/middle page
				end = mid-1;
			}
			else if(compareToMinValue>0){
				//else if it is greater than the minimum
				// compare the primary value to the max value
				int compareToMaxValue = compare(primaryKeyValue,maxValue,primaryKeyType);
				if(compareToMaxValue==0){
					throw new DBAppException("The primary key already exists!");
				}
				else if(compareToMaxValue>0){
					//If it is grater than the maximum value then we have two cases
					
					//First Case:
					//if it is less than the minimum of the next page then we know it has to
					//be inserted in the last overflow of the current page(if exists)
					int next = mid +1;
					//if we are not already on the last page
					if(next<=totalNumberOfPages) {
						Vector<Hashtable<String, Object>> nextPage = readPageIntoVector(tableName + "[" + next + "](0).class");
						Object minofnextpage = nextPage.get(0).get(primaryKey);
						int compareToMinOfNextPage = compare(primaryKeyValue, minofnextpage, primaryKeyType);
						if (compareToMinOfNextPage < 0) {
							// it must be inserted at the end of the last overflow of this page
							return tableName + "[" + mid + "](" + numberOfOverFlows + ").class";
						} else if (compareToMinOfNextPage == 0) {
							//if it is equal to the minimum of the next page then we found it
							throw new DBAppException("The primary key already exists!");
						} else {
							// or if its is not then we take the first half of the pages
							start = mid + 1;
						}
					}else{ 
						// if we are already on the last page(no next page) and the key is greater than the maximum
						// return last overflow of the current page which is the last page
						return tableName + "[" + mid + "](" + numberOfOverFlows + ").class";
					}
				}else if(compareToMaxValue<0){
					//if the primary value is less than the maximum then its in this page or one of its overflows	
					//if there is no overflows we return this page
					if(numberOfOverFlows==0){
						return tableName + "[" + mid + "](0).class";
					}else {
						// else we binary search on the page and its overflows to find the correct page
						String overFlowResult = binarySearchOnOverflowPages(tableName, numberOfOverFlows,
								primaryKey, primaryKeyValue, primaryKeyType, mid);
						return overFlowResult;
					}
				}
			}
		}

		return "";
	}

	//searching for the overflow page needed to store the new input
		public String binarySearchOnOverflowPages(String tableName, int totalNumberOfOverflowPages, String primaryKey,
														 Object primaryKeyValue, String primaryKeyType, int page) throws DBAppException{
			int start = 0;
			int end = totalNumberOfOverflowPages;
			while (start<=end) {
				int mid = (start + end) / 2;
				//here after getting the middle page of the overflow pages of this specific page we get its path
				//getting this over flow page to check if its in the range of this overflow page
				Vector<Hashtable<String,Object>> currentPage = readPageIntoVector(tableName + "[" + page + "](" + mid + ").class");

				//max value of this overflow page
				Object maxValue = currentPage.get(currentPage.size()-1).get(primaryKey);
				//min value of this overflow page
				Object minValue = currentPage.get(0).get(primaryKey);

				//comparing the primary key value to the minimum value
				int compareToMinValue = compare(primaryKeyValue,minValue,primaryKeyType);
				if(compareToMinValue==0){
					throw new DBAppException("Primary key already exists!");
				}
				//if the primary value < minimum value we take the half before the mid
				else if(compareToMinValue<0){
					end=mid-1;
				}else{
					// else we compare it to the max value
					int compareToMaxValue = compare(primaryKeyValue,maxValue,primaryKeyType);
					if(compareToMaxValue==0){
						throw new DBAppException("Primary key already exists!");
					}
					// if it is less than the maximum value then its in in this over flow page
					else if(compareToMaxValue<0){
						return tableName + "[" + page + "](" + mid + ").class";
					}
					//if it is greater than the max val then we have two cases
					else{
						//if it is less than the min of the next page 
						//then it has to be inserted in this current page
						int next = mid +1;
						if(next<=totalNumberOfOverflowPages) { //if we are not in the last page
							Vector<Hashtable<String, Object>> nextPage = readPageIntoVector(tableName + "[" + page + "](" + mid + ").class");
							Object minOfNextPage = nextPage.get(0).get(primaryKey);
							int compareToMinOfNextPage = compare(primaryKeyValue, minOfNextPage, primaryKeyType);
							if(compareToMinOfNextPage<0){
								//insert in the this page
								return tableName + "["+page+"]("+mid+").class";
							}else if(compareToMinOfNextPage==0){
								throw new DBAppException("The primary key already exists!");
							}else{
								// if it is not less than the minimum of the next page then we continue binary search
								start = mid + 1;
							}
						}else {
							//if we are already in the last page and the key is greater than its maximum then it should
							//be inserted in this last page
							return  tableName+ "["+page+"]("+mid+").class";
						}
					}
				}
			}
			return "";
		}
		
		// comparing objects
		public int compare(Object obj1, Object obj2, String primaryKeyType){

			if(primaryKeyType.equals("java.lang.Double")){
				if(((double)obj1)>((double)obj2))
					return 1;
				else if(((double)obj1)<((double)obj2))
					return -1;
				else
					return 0;
			}
			else if(primaryKeyType.equals("java.lang.Integer")){
				if(((int)obj1)>((int)obj2))
					return 1;
				else if(((int)obj1)<((int)obj2))
					return -1;
				else
					return 0;
			}
			else if(primaryKeyType.equals("java.util.Date")){
				return 	((Date)obj1).compareTo((Date)obj2);
			}
			else if(primaryKeyType.equals("java.lang.String")){
				return ((String)obj1).compareTo((String)obj2);
			}
			return -100;
		}	

	//count the pages for a specific table
	public int countNumberOfPagesWithoutOverflows(String Tablename){
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
	public int countNumberOfPageOverflows(String Tablename, int Number){

		File dir = new File("src\\main\\resources\\data");
		File[] directoryListing = dir.listFiles();
		int counter =0;
		if (directoryListing != null) {
			for (File page : directoryListing) {
				String tableName=getFileTableName(page.getName());
				int overflowNumber = getFileOverflowNumber(page.getName());
				int pageNumber = getFilePageNumber(page.getName());

				if(tableName.equals(Tablename) && pageNumber==Number && overflowNumber!=0) {
					counter++;
				}
			}
		}
		return counter;
	}
	
	public Vector<Hashtable<String,Object>> readPageIntoVector(String pageName){
		String path = "src\\main\\resources\\data\\" + pageName;
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

		return  v;
	}

}
