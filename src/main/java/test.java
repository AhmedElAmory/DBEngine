import javax.management.ObjectName;
import java.io.*;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class test {

	Hashtable<String,ArrayList<Grid>> allIndexes;

	public test(){
		allIndexes =  new Hashtable<String,ArrayList<Grid>>();
		//Read indices into memory
		String path = "src\\main\\resources\\indices.class";
		if(new File(path).exists()){
			try {
				FileInputStream fileIn = new FileInputStream(path);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				this.allIndexes = (Hashtable) in.readObject();
				in.close();
				fileIn.close();
			} catch (IOException | ClassNotFoundException i) {
				i.printStackTrace();
			}
		}
	}
	public static int  changeStringToInt(String input) {
		int base= 10000000;
		int res =0;
		int count=0;
		while (base >=1&&count <input.length()) {
			res=res+input.charAt(count)*base;
			base=base/10;
			count++;
		}
		return res;
	}
	public static void main(String[] args) throws DBAppException {
		DBApp db= new DBApp();
		//Vector<Hashtable<String, Object>> x =db.readPageIntoVector("students[1](0).class");
	Vector<BucketItem> x =db.readBucketIntoVector("Bstudents{7}[1](0).class");
		System.out.println(x.toString());
		Vector<BucketItem> z =db.readBucketIntoVector("Bstudents{7}[2](0).class");
		System.out.println(z.toString());
//		testing();
//		hashtableTest();
		//Testing();
//		testInsertion();

//		Hashtable<String,Object> a = new Hashtable<String,Object>();
//		a.put("id","1");
//		a.put("gpa",1.0);
//		a.put("first_name","mohamed");
//		page.add(a);
//
//		try {
//			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\students[1](0).class" );
//			ObjectOutputStream out = new ObjectOutputStream(fileOut);
//			out.writeObject(page);
//			out.close();
//			fileOut.close();
//		} catch (IOException i) {
//			i.printStackTrace();
//		}

//		test t1 = new test();
//		String[] arr = {"gpa","student_id"};
//		t1.createIndex("transcripts",arr);

		//System.out.println(t1.allIndexes.get("transcripts").get(0).namesAndLevels.get("gpa"));

//		Vector<BucketItem> page = new Vector<BucketItem>();
//		page=DBApp.readBucketIntoVector("Bstudents{2}[0][0](0).class");
//		System.out.println(page);
//		System.out.println(t1.allIndexes.get("students").get(1).ranges[0][8].max);
//		System.out.println(t1.allIndexes.get("students").get(1).ranges[0][8].min);
////
//		System.out.println(t1.allIndexes);
	//	System.out.println(t1.allIndexes);

//		Hashtable<String,ArrayList<Grid>> allIndexes=new Hashtable<String,ArrayList<Grid>>();
//		String path = "src\\main\\resources\\indices.class";
//		try {
//			FileInputStream fileIn = new FileInputStream(path);
//			ObjectInputStream in = new ObjectInputStream(fileIn);
//			allIndexes = (Hashtable) in.readObject();
//			in.close();
//			fileIn.close();
//		} catch (IOException | ClassNotFoundException i) {
//			i.printStackTrace();
//		}
//		System.out.println(allIndexes);
//		System.out.println(allIndexes.get("pcs").get(0).namesAndLevels);



		//DBApp x = new DBApp();

//		ArrayList<String> a = new ArrayList<String>();
//		a.add("aa");
//		a.add("bb");
//
//		System.out.println(a.contains("aa")? "sdsds":"asasaasas");

//		Hashtable<String, ArrayList<String>> a = new Hashtable<String, ArrayList<String>>();
//
//		if(a.get("asas")==null){
//			System.out.println("dssd");
//		}



//		Vector<Hashtable<String, Object>> page = new Vector<Hashtable<String,Object>>();
//		
//		Hashtable<String, Object> a = new Hashtable<String, Object>();
//		a.put("name", "mohamed");
//		a.put("age", 15);
//		
//		page.add(a);
//		
//		
//		try {
//			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\tessst.class");
//			ObjectOutputStream out = new ObjectOutputStream(fileOut);
//			out.writeObject(page);
//			out.close();
//			fileOut.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		//Vector<Hashtable<String, Object>> page2 = DBApp.readPageIntoVector("tessst");
		
		
		

		//System.out.println(page);

		
//		Object[] arr= new Object[10];
//
//		int x=2;
//
//		goDeeper(arr,x-1);
//
//		goDeeperCheck(arr,x-1);
//
//
//		String mind="2003-02-28";
//		String maxd="2003-03-01";
//
//		String minDates[] =mind.split("-");
//		String maxDates[] =maxd.split("-");
//
//		LocalDate minDate= LocalDate.of(Integer.parseInt(minDates[0]), Integer.parseInt(minDates[1]),
//				Integer.parseInt(minDates[2]));
//
//		LocalDate maxDate= LocalDate.of(Integer.parseInt(maxDates[0]), Integer.parseInt(maxDates[1]),
//				Integer.parseInt(maxDates[2]));
//
//		long daysBetween = ChronoUnit.DAYS.between(minDate, maxDate);
//
//		minDate=minDate.plusDays(20);
//		System.out.println(minDate.toString() );
//
//		System.out.println("Days: " + daysBetween);
//
//
//		Range ranges[][]=new Range[1][10];
//
//		String min="aaaaa";
//		String max="zzzzz";
//
//
//		String standard="";
//		for(int i=0;i<Math.max(min.length(), max.length());i++) {
//			if(i<min.length()&&min.charAt(i)==max.charAt(i)) {
//				standard+=min.charAt(i);
//			}else if(i<min.length()&&min.charAt(i)!=max.charAt(i)) {
//				int dif=max.charAt(i)-min.charAt(i);
//				int range=dif/10;
//
//				boolean flag=false;
//
//				ranges[0][0]=new Range(min,standard+(char)(min.charAt(i)+range));
//				range+=dif/10;
//				for(int j=1;j<9;j++) {
//					ranges[0][j]=new Range(ranges[0][j-1].max,standard+(char)(min.charAt(i)+range));
//					range+=dif/10;
//				}
//				ranges[0][9]=new Range(ranges[0][8].max,max);
//				break;
//			}else if(i==min.length()) {
//				int dif=max.charAt(i);
//				int range=dif/10;
//
//				ranges[0][0]=new Range(min,standard+(char)(min.charAt(i)+range));
//				range+=dif/10;
//				for(int j=1;j<9;j++) {
//					ranges[0][j]=new Range(ranges[0][j-1].max,standard+(char)(min.charAt(i)+range));
//					range+=dif/10;
//				}
//				ranges[0][9]=new Range(ranges[0][8].max,max);
//				break;
//			}
//		}
//
//		for(int i=0;i<10;i++) {
//			System.out.println(ranges[0][i].min+"     "+ranges[0][i].max);
//		}

		
	}

	public static void updateCSV( String strTableName, String[] strarrColName)  {

		ArrayList<String> listOfCol = new ArrayList<String>();
		for(int i=0; i<strarrColName.length ;i++){
			listOfCol.add(strarrColName[i]);
		}

		//Read csv to arraylist of arrays
		ArrayList<String[]> arr = new ArrayList<String[]>();
		BufferedReader csvReader = null;
		try {
			csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String row = csvReader.readLine();
			while ( row!= null) {
				String[] data = row.split(",");
				arr.add(data);
				row = csvReader.readLine();
			}
			csvReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//loop on arraylist to edit needed rows
		for(int i=0; i<arr.size();i++){

			//check if this is needed row
			if(arr.get(i)[0].equals(strTableName) && listOfCol.contains(arr.get(i)[1])){
				arr.get(i)[4]="true";
			}
		}

		//Now write arraylist to csv file again
		try {
			FileWriter csvWriter = new FileWriter("src\\main\\resources\\metadata.csv");
			csvWriter.append("");  //to clear csv file first

			csvWriter = new FileWriter("src\\main\\resources\\metadata.csv", true);
			//Loop over arraylist
			for(int i=0; i<arr.size();i++){
				if(arr.get(i).length==7) {
					csvWriter.append("\n" + arr.get(i)[0] + "," + arr.get(i)[1] + "," + arr.get(i)[2] + "," + arr.get(i)[3] + "," +
							arr.get(i)[4] + "," + arr.get(i)[5] + "," + arr.get(i)[6]);
				}
			}
			csvWriter.flush();
			csvWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}



	}


	public static String checkCreateIndexExceptions(String strTableName, String[] strarrColName) throws DBAppException {

		String primaryColumn="";
		String primaryDataType="";
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();

			// check if column names in hashtable exist in metadata
			int countCorrectColumns = 0;
			boolean tableExist = false;
			ArrayList<String> listOfCol = new ArrayList<String>();
			for(int i=0; i<strarrColName.length ;i++){
				listOfCol.add(strarrColName[i]);
			}

			while (current != null) {
				String arr[] = current.split(",");
				// check if metadata row has same table name as input
				if (arr[0].equals(strTableName)) {
					tableExist=true;
					// check if hashtable contains same column name as metadata
					if (listOfCol.contains(arr[1])) {
						countCorrectColumns++;
					}
					if(arr[3].equals("true")){
						primaryColumn=arr[1];
						primaryDataType=arr[2];
					}

				}
				current = br.readLine();
			}
			br.close();
			if (!tableExist) {
				throw new DBAppException("Table does not exist!");
			}
			if (!(countCorrectColumns == listOfCol.size())) {
				throw new DBAppException("Array Columns are not in metadata!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return primaryColumn+","+primaryDataType;
	}
	
	public static void goDeeper(Object[] array,int n) {
		if(n==0) {
			for(int i=0;i<10;i++) {
				array[i]=i;
			}
			return;
		}
		
		for(int i=0;i<10;i++) {
			array[i]=new Object[10];
			goDeeper((Object[])array[i],n-1);
		}
		
	}
	//goDeeperCheck has array, level, hashtable with level,value
	public static void goDeeperCheck(Object[] array,int n) {
		if(n==0) {
			for(int i=0;i<10;i++) {
//				System.out.println((int)array[i]);
			}
		}else {
			for(int i=0;i<10;i++) {
				goDeeperCheck((Object[])array[i],n-1);
			}
		}
	}
	
	public static void testing() {
		ArrayList<String> operatorsList = new ArrayList<>();
		operatorsList.add("XOR");
		operatorsList.add("XOR");
		operatorsList.add("AND");
		operatorsList.add("OR");
		operatorsList.add("OR");
		operatorsList.add("XOR");
		operatorsList.add("OR");
		operatorsList.add("XOR");
		operatorsList.add("AND");
		operatorsList.add("AND");
		operatorsList.add("OR");
		operatorsList.add("XOR");
		operatorsList.add("OR");
		operatorsList.add("AND");
		operatorsList.add("AND");
		operatorsList.add("XOR");
		System.out.println(operatorsList);
		ArrayList<String> collection = new ArrayList<>();
		collection.add("AND");
		operatorsList.removeAll(collection);
		System.out.println(operatorsList);
	}
	
	public static void hashtableTest() {

	    Hashtable<String, Integer> hashtable = new Hashtable<String, Integer>();
	    Hashtable<String, Integer> hashtable2 = new Hashtable<String, Integer>();

	    hashtable.put("mike" , 1);
	    hashtable.put("Lisa" ,2);
	    hashtable.put("Louis" , 3);
	    hashtable.put("Chris" ,4);
	    hashtable.put("Chuck" , 5);
	    hashtable.put("Kiril" ,6);

	    /* table 2 values */    
	    hashtable2.put("Louis" , 1);
	    hashtable2.put("samy" ,2);
	    hashtable2.put("Mo" , 3);
	    hashtable2.put("lolo" , 4);
	    hashtable2.put("Chuck" ,5);
	    hashtable2.put("samual" ,6);
	    
	    Hashtable<String, Integer> inersect = new Hashtable<String, Integer>(hashtable);
	    inersect.keySet().retainAll(hashtable2.keySet());
	    System.out.println(inersect);
	    
	    //min index 0;
	    //max index 10;
	    //check if there is a constraint on the greater than or less than and modify the min and max index
	    //go into all the blocks between the two ranges
	    //go to the two ranges but make sure to go lineary through them at the end to check for the greater than or less than constraints
	}
	
	public static void Testing() throws DBAppException {
		DBApp amory=new DBApp();
		
//		for(Grid x :(ArrayList<Grid>)amory.allIndexes.get("students")) {
//			System.out.println(x.toString());
//		}
		
		//System.out.println(amory.readBucketIntoVector("BAmory{0}[0](0).class"));
//		createTableTest(amory);
//		insertDataTest(amory);
//		selectTest(amory);
		universitySelectTest(amory);
//		insertTest(amory);
//		System.out.println(amory.readBucketIntoVector("BAmory{0}[0](0).class"));
		//deleteTest(amory);
//		updateTest(amory);
//		createIndexTest(amory);
//		Hashtable<String, Object> a = new Hashtable<>();
//		a.put("asas", null);
	}
	
	public static void createTableTest(DBApp app) throws DBAppException {
		Hashtable<String,String> htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		
		Hashtable<String,String> htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "1");
		htblColNameMin.put("name", "AAAAA");
		htblColNameMin.put("gpa", "0.7");
		
		Hashtable<String,String> htblColNameMax = new Hashtable();
		htblColNameMax.put("id", "999999999");
		htblColNameMax.put("name", "zzzzzzzzz");
		htblColNameMax.put("gpa", "5.0");
		app.createTable("Amory", "id", htblColNameType, htblColNameMin, htblColNameMax);
	}
	
	public static void insertDataTest(DBApp app) throws DBAppException {
		Hashtable<String,Object> htblColNameValue = new Hashtable( );
		
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		
		app.insertIntoTable( "Amory" , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 453455 ));
		htblColNameValue.put("name", new String("Sawy Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.2 ) );
		
		app.insertIntoTable( "Amory" , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5674567 ));
		htblColNameValue.put("name", new String("Dalia Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.25 ) );
		
		app.insertIntoTable( "Amory" , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23498 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		
		app.insertIntoTable( "Amory" , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 78412 ));
		htblColNameValue.put("name", new String("Zaky Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.88 ) );
		
		app.insertIntoTable( "Amory" , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 1111 ));
		htblColNameValue.put("name", new String("Sherif Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.99 ) );
		
		app.insertIntoTable( "Amory" , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 2222 ));
		htblColNameValue.put("name", new String("Belo Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.7 ) );
		
		app.insertIntoTable( "Amory" , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 3333 ));
		htblColNameValue.put("name", new String("WAWA Noor" ) );
		htblColNameValue.put("gpa", new Double( 3.2 ) );
		
		app.insertIntoTable( "Amory" , htblColNameValue );
	}
	
	public static void createIndexTest(DBApp app) throws DBAppException {

		String[] arr={"id"};
		app.createIndex("Amory", arr);
	}
	
	public static void selectTest(DBApp app) throws DBAppException{
		SQLTerm term1 = new SQLTerm();
		SQLTerm term2 = new SQLTerm();
		SQLTerm term3 = new SQLTerm();
		SQLTerm term4 = new SQLTerm();
		SQLTerm term5 = new SQLTerm();
		
		term1._strTableName = "Amory";
		term1._strColumnName= "id";
		term1._strOperator = "<=";
		term1._objValue = new Integer( 1111 );
		
		term2._strTableName = "Amory";
		term2._strColumnName= "id";
		term2._strOperator = ">=";
		term2._objValue = new Integer( 3333 );
		
//		term2._strTableName = "Amory";
//		term2._strColumnName= "gpa";
//		term2._strOperator = "=";
//		term2._objValue = new Double( 0.95 );
		
//		term3._strTableName = "Amory";
//		term3._strColumnName= "id";
//		term3._strOperator = "=";
//		term3._objValue = new Integer( 78452 );
		
		SQLTerm[] arrSQLTerms= {term1,term2};
		
		String[]strarrOperators = {"AND"};
		
		Iterator resultSet = app.selectFromTable(arrSQLTerms , strarrOperators);
		if(resultSet==null) {
			System.out.println("null");
		}else {
			if(resultSet.hasNext()) {
				while(resultSet.hasNext()) {
					System.out.println(resultSet.next());
				}
			}else {
				System.out.println("This is an empty result set");
			}
		}
	
	}
	
	public static void universitySelectTest(DBApp app) throws DBAppException{
		SQLTerm term1 = new SQLTerm();
		SQLTerm term2 = new SQLTerm();
		SQLTerm term3 = new SQLTerm();
		SQLTerm term4 = new SQLTerm();
		SQLTerm term5 = new SQLTerm();
		
		term1._strTableName = "students";
		term1._strColumnName= "id";
		term1._strOperator = ">=";
		term1._objValue = new String("46-0000");
		
		term2._strTableName = "students";
		term2._strColumnName= "id";
		term2._strOperator = "<=";
		term2._objValue = new String( "46-9999" );
		
		term4._strTableName = "students";
		term4._strColumnName= "id";
		term4._strOperator = "<=";
		term4._objValue = new String( "49-9999" );
		
		
//		term2._strTableName = "Amory";
//		term2._strColumnName= "gpa";
//		term2._strOperator = "=";
//		term2._objValue = new Double( 0.95 );
		
		term3._strTableName = "students";
		term3._strColumnName= "gpa";
		term3._strOperator = ">";
		term3._objValue = new Double(1.0);
		
		SQLTerm[] arrSQLTerms= {term1,term2,term4,term3};
		
		String[]strarrOperators = {"AND","OR","XOR"};
		
		Iterator resultSet = app.selectFromTable(arrSQLTerms , strarrOperators);
		if(resultSet==null) {
			System.out.println("null");
		}else {
			if(resultSet.hasNext()) {
				while(resultSet.hasNext()) {
					System.out.println(resultSet.next());
				}
			}else {
				System.out.println("This is an empty result set");
			}
		}
	
	}
	
	
	
	public static void updateTest(DBApp app) throws DBAppException{
		Hashtable<String,Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put("gpa", new Double( 1.5 ));
		app.updateTable("Amory", "78452" , htblColNameValue);
		
		
	}

	
	public static void insertTest(DBApp app) throws DBAppException{
		Hashtable<String,Object> row3 = new Hashtable<>();
		row3.put("id",233);
		app.insertIntoTable("Amory",row3);
	}

	public static void testInsertion() throws DBAppException {

		DBApp db = new DBApp();

//	Hashtable<String,String> colNameType = new Hashtable<>();
//	colNameType.put("id","java.lang.Integer");
//	colNameType.put("name","java.lang.String");
//	colNameType.put("age","java.lang.Integer");
//
//	Hashtable<String,String> colNameMin = new Hashtable<>();
//	colNameMin.put("id","0");
//	colNameMin.put("name","AAAAAA");
//	colNameMin.put("age","0");
//
//	Hashtable<String,String> colNameMax = new Hashtable<>();
//	colNameMax.put("id","999");
//	colNameMax.put("name","zzzzzz");
//	colNameMax.put("age","999");
//
//	db.createTable("students","id",colNameType,colNameMin,colNameMax);
//
//	String[] aa = {"id"};
//	db.createIndex("students",aa);

	Hashtable<String,Object> row3 = new Hashtable<>();
	row3.put("id",19);
	row3.put("name","aka");
	row3.put("age",66);
	db.insertIntoTable("students",row3);
	//db.deleteFromTable("students",row3);

//	1-5-10-20-35-15-27-21-19-34-0-29-4-14-9-13
	System.out.println(DBApp.readPageIntoVector("students[1](0).class"));
//	System.out.println(DBApp.readPageIntoVector("students[1](1).class"));
//	System.out.println(DBApp.readPageIntoVector("students[1](2).class"));
//	System.out.println(DBApp.readPageIntoVector("students[2](0).class"));
//	System.out.println(DBApp.readPageIntoVector("students[3](0).class"));
	System.out.println(Grid.readBucketIntoVector("Bstudents{1}[0](0).class"));
//	System.out.println(Grid.readBucketIntoVector("Bstudents{0}[0](1).class"));
//	System.out.println(Grid.readBucketIntoVector("Bstudents{0}[0](2).class"));
//	System.out.println(Grid.readBucketIntoVector("Bstudents{0}[0](3).class"));
	}

	
	public static void deleteTest(DBApp app) throws DBAppException{
		Hashtable<String,Object> htblColNameValue = new Hashtable( );
		
		htblColNameValue.put("id", 233 );
		app.deleteFromTable("Amory", htblColNameValue);
	}
}




