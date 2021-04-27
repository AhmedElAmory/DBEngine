import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.Hashtable;
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
		
		String tableName = "wawa";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
        
		
		
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
//
//
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
//					new FileOutputStream("src\\main\\resources\\data\\"+"courses"+"[3](0)"+".class");
//			ObjectOutputStream out = new ObjectOutputStream(fileOut);
//			out.writeObject(v4);
//			out.close();
//			fileOut.close();
//			System.out.println("src\\main\\resources\\data\\"+"courses"+"[3](0)"+".class");
//		} catch (IOException i) {
//			i.printStackTrace();
//		}

//
//
//		Vector<Hashtable<String,Object>> vx = ReadPageIntoVector("courses[3](0).class");
//	      System.out.println(vx.toString());


//		System.out.println(binarySearchOnPages("courses",3,"course_id",32,"java.lang.Integer"));
		
		Date x= new Date();
		

//		String a = "abc";
//		String b = "ASS";
//
//		System.out.println(a.compareTo(b));
//



		//System.out.println(compare(10,20,"java.lang.Integer" ));


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

	public static int compare(Object obj1, Object obj2, String primarykeyType){

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

	public static Vector<Hashtable<String,Object>> ReadPageIntoVector(String pageName){

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


	//searching for the overflow page needed to store the new input
	public static String binarySearchOnOverflowPages(String TableName, int TotalNumberOfOverflowPages, String PrimaryKey,
													 Object PrimaryKeyValue, String primarykeyType, int page) throws DBAppException{
		int startvalue = 0;
		int endvalue = TotalNumberOfOverflowPages;
		int mid;
		while (startvalue<=endvalue) {

			mid = (startvalue + endvalue) / 2;
			//here after getting the middle page of the overflow pagesof this specific page we get its path
			//getting the this over flow page to check if its in the range of this overflow page
			Vector<Hashtable<String,Object>> v = ReadPageIntoVector(TableName + "[" + page + "](" + mid + ").class");

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
				//if it is greater than the max val then we have two cases
				else{
					//if it is less than the min of the next page then it has to be inserted in this current page
					int temp = mid +1;
					if(temp<=TotalNumberOfOverflowPages) { //if we are not in the last page
						Vector<Hashtable<String, Object>> v5 = ReadPageIntoVector(TableName + "[" + page + "](" + mid + ").class");
						Object minofnextpage = v5.get(0).get(PrimaryKey);
						int comp5 = compare(PrimaryKeyValue, minofnextpage, primarykeyType);
						if(comp5<0){
							//insert in the next page
							return TableName + "["+page+"]("+temp+").class";
						}else if(comp5==0){
							throw new DBAppException("The primary key already exists!");
						}else{
							// if it is not less than the min of the next page then we continue binary search
							startvalue = mid + 1;
						}

					}else {
						//if we are already in the last page and the key is greater than its maximum then it should
						//be inserted in this last page
						return  TableName+ "["+page+"]("+mid+").class";
					}
				}
			}
		}
		return "";
	}



	public static String binarySearchOnPages(String TableName, int TotalNumberOfPages, String PrimaryKey, Object PrimaryKeyValue, String primarykeyType) throws DBAppException {

		int startvalue = 1;
		int endvalue = TotalNumberOfPages;
		int mid;

		//checking if the value of the input less than the min val in the table and returning the first page
		Vector<Hashtable<String,Object>> v3 = ReadPageIntoVector(TableName+"[1](0).class");

		Object minValue3 = v3.get(0).get(PrimaryKey);
		int comp3 = compare(PrimaryKeyValue,minValue3,primarykeyType);
		if(comp3<0){
			return TableName + "[1](0).class";
		}

		// binary search for the page

		while (startvalue<=endvalue){

			mid = (startvalue + endvalue)/2;

			Vector<Hashtable<String,Object>> v = ReadPageIntoVector(TableName + "[" + mid + "](0).class");

			Object maxValue = null;
			// getting the min val of the page
			Object minValue = v.get(0).get(PrimaryKey);

			//getting the number of overflow pages of this page
			int numberofoverflows = countNumberOfPagesWithOverflow(TableName,mid);

			//if its a zero then it does not have overflows then its max is in this page in the end
			if (numberofoverflows==0){
				maxValue = v.get(v.size()-1).get(PrimaryKey);
			}else{
				//else we get the last overflow page and get its last value to be the max val
				Vector<Hashtable<String,Object>> v2 = ReadPageIntoVector(TableName + "[" + mid + "](" + numberofoverflows + ").class");

				maxValue= v2.get(v2.size()-1).get(PrimaryKey);
			}
			// we compare  the primary val to the min val
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
				//if it is grater than the max val then we have two cases
				else if(comp2>0){

					//if it is less than the minimum of the next page then we know it has to
					// be inserted in the last overflow of the current page
					int temp = mid +1;

					//if we are not already on the last page
					if(temp<=TotalNumberOfPages) {
						Vector<Hashtable<String, Object>> v5 = ReadPageIntoVector(TableName + "[" + temp + "](0).class");
						Object minofnextpage = v5.get(0).get(PrimaryKey);
						int comp5 = compare(PrimaryKeyValue, minofnextpage, primarykeyType);
						if (comp5 < 0) {
							// it must be inserted at the end of the last overflow of this page
							return TableName + "[" + mid + "](" + numberofoverflows + ").class";
						} else if (comp5 == 0) {
							//if it is equal to the minimum of the next page then we found it
							throw new DBAppException("The primary key already exists!");
						} else {
							// or if its is not then we take the first half of the pages
							startvalue = mid + 1;
						}
					}else{ // if we are already on the last page(no next page) and the key is greater than the maximum
						// return last overflow of the current page which is the last page
						return TableName + "[" + mid + "](" + numberofoverflows + ").class";
					}

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

	public static int countNumberOfPagesWithOverflow(String Tablename, int Number){

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