import javax.management.ObjectName;
import java.io.*;
import java.lang.reflect.Array;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class test {

	Hashtable<String,ArrayList<Grid>> allIndexes;

	public test(){

		allIndexes =  new Hashtable<String,ArrayList<Grid>>();
	}

	public static void main(String[] args) throws DBAppException {






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

	//	test t1 = new test();
//		String[] arr = {"gpa","student_id"};
//		t1.createIndex("transcripts",arr);

		//System.out.println(t1.allIndexes.get("transcripts").get(0).namesAndLevels.get("gpa"));

//		Vector<BucketItem> page = new Vector<BucketItem>();
//		page=DBApp.readBucketIntoVector("Btranscripts[0][4](0).class");
//		System.out.println(page);

//		System.out.println(t1.allIndexes);

		Hashtable<String,ArrayList<Grid>> allIndexes=new Hashtable<String,ArrayList<Grid>>();
		String path = "src\\main\\resources\\indices.class";
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			allIndexes = (Hashtable) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			i.printStackTrace();
		}
		System.out.println(allIndexes);
		System.out.println(allIndexes.get("pcs").get(0).namesAndLevels);



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


	// following method creates one index – either multidimensional
	// or single dimension depending on the count of column names passed.
//	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
//		String primarycolAndDataType = checkCreateIndexExceptions(strTableName,strarrColName);
//		String[] x = primarycolAndDataType.split(",");
//		String primaryCol = x[0];
//		String primaryDataType = x[1];
//		Grid index = new Grid(strTableName,strarrColName,primaryCol,primaryDataType);
//		ArrayList<Grid> a = allIndexes.get(strTableName);
//		if(a==null){
//			a =new ArrayList<Grid>();
//			a.add(index);
//			allIndexes.put(strTableName,a);
//		}else{
//			a.add(index);
//		}
//
//		try {
//			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\indices.class");
//			ObjectOutputStream out = new ObjectOutputStream(fileOut);
//			out.writeObject(allIndexes);
//			out.close();
//			fileOut.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		//After creating an index we have to edit the metadata to indicate that an index is created on specific columns
//		updateCSV(strTableName,strarrColName);
//
//	}

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
}
