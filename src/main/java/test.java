import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import org.junit.jupiter.api.Assertions;

public class test {
	public static void main(String[] args) throws DBAppException {
//		try {
////			String wawa="lol";
////			Integer mama=2;
////			Object lol=(Object)2.4657;
////			System.out.println(lol.getClass());
////			System.out.println(Class.forName("java.lang.Double").isInstance(lol));
//			
//		} catch (IllegalArgumentException | SecurityException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		DBApp dbApp=new DBApp();
		
		String tableName = "wawrrr";
//
//        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//        htblColNameType.put("gpa", "java.lang.Double");
//        htblColNameType.put("student_id", "java.lang.String");
//        htblColNameType.put("course_name", "java.lang.String");
//        htblColNameType.put("date_passed", "java.util.Date");
//
//        Hashtable<String, String> minValues = new Hashtable<>();
//        minValues.put("gpa", "0.7");
//        minValues.put("student_id", "43-0000");
//        minValues.put("course_name", "AAAAAA");
//        minValues.put("date_passed", "1990-01-01");
//
//        Hashtable<String, String> maxValues = new Hashtable<>();
//        maxValues.put("gpa", "5.0");
//        maxValues.put("student_id", "99-9999");
//        maxValues.put("course_name", "zzzzzz");
//        maxValues.put("date_passed", "202012-31");
//
//        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
        
        Hashtable<String, Object> nameValue = new Hashtable<>();
        nameValue.put("gpa", 3.0);
        nameValue.put("date_passed", new Date(1999,1,1));
        nameValue.put("course_name", "AAA");
        
        
        dbApp.insertIntoTable("wawa", nameValue);
        
		
		
//		System.out.println(getFileOverflowNumber("Employee[243](1423)"));
		
//		File dir = new File("src\\main\\resources");
//		File[] directoryListing = dir.listFiles();
//		Boolean found=false;
//		if (directoryListing != null) {
//			boolean foundPage=false;
//			for (File page : directoryListing) {
//				String name=page.getName();
//				System.out.println(name);
//			}
//		}
		
		
//		DBApp dbApp = new DBApp();
//		String tableName = "courses";
//
//        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//        htblColNameType.put("date_added", "java.util.Date");
//        htblColNameType.put("course_id", "java.lang.String");
//        htblColNameType.put("course_name", "java.lang.String");
//        htblColNameType.put("hours", "java.lang.Integer");
////
////
//        Hashtable<String, String> minValues = new Hashtable<>();
//        minValues.put("date_added", "1990-01-01");
//        minValues.put("course_id", "100");
//        minValues.put("course_name", "AAAAAA");
//        minValues.put("hours", "1");
////
//        Hashtable<String, String> maxValues = new Hashtable<>();
//        maxValues.put("date_added", "2000-12-31");
//        maxValues.put("course_id", "2000");
//        maxValues.put("course_name", "zzzzzz");
//        maxValues.put("hours", "24");
//
//        dbApp.createTable(tableName, "course_id", htblColNameType, minValues, maxValues);
		
//        dbApp.init();
//
//        String table = "courses";
//        Hashtable<String, Object> row = new Hashtable();
//
//
//        row.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//        row.put("course_id", "1");
//        row.put("course_name", "bar");
//        row.put("hours", 13);
//
//		Hashtable<String, Object> row1 = new Hashtable();
//
//		row1.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row1.put("course_id", "2");
//		row1.put("course_name", "bar");
//		row1.put("hours", 13);
//
//		Hashtable<String, Object> row2 = new Hashtable();
//
//		row2.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row2.put("course_id", "3");
//		row2.put("course_name", "bar");
//		row2.put("hours", 13);
//		Hashtable<String, Object> row3 = new Hashtable();
//
//		row3.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row3.put("course_id", "4");
//		row3.put("course_name", "bar");
//		row3.put("hours", 13);
//
//		Hashtable<String, Object> row4 = new Hashtable();
//
//		row4.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row4.put("course_id", "5");
//		row4.put("course_name", "bar");
//		row4.put("hours", 13);
//
//		Hashtable<String, Object> row5 = new Hashtable();
//
//		row5.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row5.put("course_id", "6");
//		row5.put("course_name", "bar");
//		row5.put("hours", 13);
//
//		Hashtable<String, Object> row6 = new Hashtable();
//
//		row6.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row6.put("course_id", "7");
//		row6.put("course_name", "bar");
//		row6.put("hours", 13);
//
//		Hashtable<String, Object> row7 = new Hashtable();
//		row7.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row7.put("course_id", "8");
//		row7.put("course_name", "bar");
//		row7.put("hours", 13);
//
//		Hashtable<String, Object> row8 = new Hashtable();
//		row8.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row8.put("course_id", "9");
//		row8.put("course_name", "bar");
//		row8.put("hours", 13);
//
//		Hashtable<String, Object> row9 = new Hashtable();
//		row9.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row9.put("course_id", "10");
//		row9.put("course_name", "bar");
//		row9.put("hours", 13);
//
//		Hashtable<String, Object> row10 = new Hashtable();
//		row10.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row10.put("course_id", "11");
//		row10.put("course_name", "bar");
//		row10.put("hours", 13);
//
//		Hashtable<String, Object> row11 = new Hashtable();
//		row11.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row11.put("course_id", "12");
//		row11.put("course_name", "bar");
//		row11.put("hours", 13);
//
//		Vector<Hashtable<String,Object>> v1 = new Vector<>();
//
//		v1.add(row);
//		v1.add(row1);
//		v1.add(row2);
//		v1.add(row3);
//
//		Vector<Hashtable<String,Object>> v2 = new Vector<>();
//
//		v2.add(row4);
//		v2.add(row5);
//		v2.add(row6);
//		v2.add(row7);
//
//		Vector<Hashtable<String,Object>> v3 = new Vector<>();
//
//		v3.add(row8);
//		v3.add(row9);
//		v3.add(row10);
//		v3.add(row11);
//
//
//		Hashtable<String, Object> row12 = new Hashtable();
//		row12.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row12.put("course_id", 31);
//		row12.put("course_name", "bar");
//		row12.put("hours", 13);
//
//		Hashtable<String, Object> row13 = new Hashtable();
//		row13.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row13.put("course_id", 34);
//		row13.put("course_name", "bar");
//		row13.put("hours", 13);
//
//
//		Hashtable<String, Object> row14 = new Hashtable();
//		row14.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row14.put("course_id", 37);
//		row14.put("course_name", "bar");
//		row14.put("hours", 13);
//
//
//		Hashtable<String, Object> row15 = new Hashtable();
//		row15.put("date_added", new Date(2011 - 1900, 4 - 1, 1));
//		row15.put("course_id", 45);
//		row15.put("course_name", "bar");
//		row15.put("hours", 13);
//
////
//		Vector<Hashtable<String,Object>> v4 = new Vector<>();
//
//		v4.add(row12);
//		v4.add(row13);
//		v4.add(row14);
//		v4.add(row15);
////
//		try {
//			FileOutputStream fileOut =
//					new FileOutputStream("src\\main\\resources\\data\\"+"courses"+"[1](0)"+".class");
//			ObjectOutputStream out = new ObjectOutputStream(fileOut);
//			out.writeObject(v4);
//			out.close();
//			fileOut.close();
//			System.out.println("src\\main\\resources\\data\\"+"courses"+"[1](0)"+".class");
//		} catch (IOException i) {
//			i.printStackTrace();
//		}

//
//
//		Vector<Hashtable<String,Object>> vx = ReadPageIntoVector("courses[3](0).class");
//	      System.out.println(vx.toString());


//		System.out.println(binarySearchOnPages("courses",1,"course_id",32,"java.lang.Integer"));
		
		String a = "abc";
		String b = "ASS";

		System.out.println(a.compareTo(b));
////mmm
///


		//System.out.println(compare(10,20,"java.lang.Integer" ));


	}

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
	
	public void checkCreateTableExceptions(String strTableName,String strClusteringKeyColumn
			,Hashtable<String, String> htblColNameType,Hashtable<String, String> htblColNameMin
			,Hashtable<String, String> htblColNameMax) throws DBAppException {
		//check if table already exists
		if(checkTableExists(strTableName)) {
			throw new DBAppException("Table already exists!");
		}
		//check inserted dataTypes
		checkDataTypeAndPrimaryExists(htblColNameType,strClusteringKeyColumn,htblColNameMin,htblColNameMax);
	}
	
	public void checkDataTypeAndPrimaryExists(Hashtable<String, String> htblColNameType,String strClusteringKeyColumn
			,Hashtable<String, String> htblColNameMin
			,Hashtable<String, String> htblColNameMax) throws DBAppException {
		
		if(htblColNameType.size()!=htblColNameMin.size()&&htblColNameType.size()!=htblColNameMax.size()) {
			throw new DBAppException("The inserted hashtables are not of same size");
		}
		
		boolean clusterFound=false;
		Set<String> keys = htblColNameType.keySet();
		for(String key: keys) {
			//Check if primary key exists
			if(!htblColNameMax.containsKey(key)) {
				throw new DBAppException("Column "+key+" has no maximum value inserted");
			}else if(!htblColNameMin.containsKey(key)) {
				throw new DBAppException("Column "+key+" has no minimum value inserted");
			}
			if(key.equals(strClusteringKeyColumn)) {
				clusterFound=true;
			}
			String dataType=htblColNameType.get(key);
			if(!(dataType.equals("java.lang.Integer")||
					dataType.equals("java.lang.String")||
					dataType.equals("java.lang.Double")||
					dataType.equals("java.util.Date"))) {
				throw new DBAppException("Column "+key+" has an unsupported data type");
			
			}else if(dataType.equals("java.lang.Integer")) {
				try {
					Integer.parseInt(htblColNameMax.get(key));
				}catch (Exception e) {
					throw new DBAppException("Column "+key+" maximum value is not an integer");
				}
				try {
					Integer.parseInt(htblColNameMin.get(key));
				}catch (Exception e) {
					throw new DBAppException("Column "+key+" minimum value is not an integer");
				}
			
			}else if(dataType.equals("java.lang.Double")) {
				try {
					Double.parseDouble(htblColNameMax.get(key));
				}catch (Exception e) {
					throw new DBAppException("Column "+key+" maximum value is not a Double");
				}
				try {
					Double.parseDouble(htblColNameMin.get(key));
				}catch (Exception e) {
					throw new DBAppException("Column "+key+" minimum value is not a Double");
				}
			
			}else if(dataType.equals("java.util.Date")) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				try {
			        format.parse(htblColNameMax.get(key));
			     }
			     catch(ParseException e){
			    	 throw new DBAppException("Column "+key+" maximum value has wrong date format, Make sure it's (YYYY-MM-DD)");
			     }
				try {
					 format.parse(htblColNameMin.get(key));
			     }
			     catch(ParseException e){
			    	 throw new DBAppException("Column "+key+" minimum value has wrong date format, Make sure it's (YYYY-MM-DD)");
			     }
			}
		}
		if(!clusterFound) {
			throw new DBAppException("Primary key not found!");
		}
	}
	
	public void checkInsertInputConstraints(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		//5 min 6 max
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
						
						int compareToMin=compare(htblColNameValue.get(arr[1]), arr[5], arr[2]);
						int compareToMax=compare(htblColNameValue.get(arr[1]), arr[6], arr[2]);
						
						if (compareToMin<0) {
							throw new DBAppException("The value inserted in column "+arr[1]+" is below the minimum value");
						}
						if (compareToMax>0) {
							throw new DBAppException("The value inserted in column "+arr[1]+" is below the minimum value");
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
	
	public static String getFileTableName(String fileName) {
		String tableName="";
		int c=0;
		while(fileName.charAt(c)!='[') {
			tableName+=fileName.charAt(c);
			c++;
		}
		return tableName;
	}
	
	public static int getFilePageNumber(String fileName) {
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
	
	public static int getFileOverflowNumber(String fileName) {
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

	public static String binarySearchOnPages(String tableName, int totalNumberOfPages, 
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
		public static String binarySearchOnOverflowPages(String tableName, int totalNumberOfOverflowPages, String primaryKey,
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
		public static int compare(Object obj1, Object obj2, String primaryKeyType){

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
	public static int countNumberOfPageOverflows(String Tablename, int Number){

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
	
	public static Vector<Hashtable<String,Object>> readPageIntoVector(String pageName){
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