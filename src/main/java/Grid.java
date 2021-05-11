import java.io.*;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Grid implements Serializable {

	private String tableName;
	private Object[] array;
	private Range[][] ranges;
	private Hashtable<String,Integer> namesAndLevels;
	private Hashtable<String,String> namesAndDataTypes;
	private String primaryColumn;
	private String primaryDataType;
	
	public Grid(String tableName,String[] strarrColName,String primaryColumn,String primaryDataType) {
		this.tableName=tableName;
		this.primaryColumn=primaryColumn;
		this.primaryDataType=primaryDataType;
		this.namesAndLevels = new Hashtable<String,Integer>();
		this.namesAndDataTypes = new Hashtable<String,String>();

		for(int i=0;i<strarrColName.length;i++) {
			namesAndLevels.put(strarrColName[i], i);
		}
		array = new Object[11];
		goDeeper(array,strarrColName.length-1);
		
		ranges = new Range[strarrColName.length][10];
		getRanges();
		populateIndex();
	}


	public void goDeeper(Object[] array, int n) {
		if(n==0) {
			return;
		}
		for(int i=0;i<11;i++) {
			array[i]=new Object[11];
			goDeeper((Object[])array[i],n-1);
		}
		
	}
	
	public void getRanges() {
		
		for(int i=0; i<namesAndLevels.size(); i++) {
		
			try {
				BufferedReader csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
				String line = csvReader.readLine();
				while (line != null) {
				    String[] data = line.split(",");
				    
				    if(data[0].equals(tableName)&&namesAndLevels.containsKey(data[1])) {
				    	
				    	String datatype = data[2];
				    	namesAndDataTypes.put(data[1],datatype);  // To populate the namesAndDataTypes hashtable
				    	String min = data[5];
				    	String max = data[6];
				    	divideRanges(namesAndLevels.get(data[1]),datatype,min,max);
				    	
				    }
				    line=csvReader.readLine();
				}
				csvReader.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		
			
			
		}
	}
	
	public void divideRanges(int level,String datatype,String min, String max) {

		if(datatype.equals("java.lang.Integer")) {
			int minimum= Integer.parseInt(min);
			int maximum= Integer.parseInt(max);
			int difference =maximum- minimum;
			int range= difference/10;
			ranges[level][0]=new Range(minimum,minimum+range);
			for (int i=1;i<9;i++) {
				int m=(int)ranges[level][i-1].max;
				ranges[level][i]=new Range(m, m+range);
			}
			ranges[level][9]=new Range(ranges[level][8].max,maximum);

		}
		else if(datatype.equals("java.lang.Double")) {
			double minimum= Double.parseDouble(min);
			double maximum= Double.parseDouble(max);
			double difference =maximum- minimum;
			double range= difference/10;
			ranges[level][0]=new Range(minimum,minimum+range);
			for (int i=1;i<9;i++) {
				double m=(double)ranges[level][i-1].max;
				ranges[level][i]=new Range(m, m+range);
			}
			ranges[level][9]=new Range(ranges[level][8].max,maximum);

		}else if(datatype.equals("java.util.Date")) {
			String minDates[] = min.split("-");
			String maxDates[] = max.split("-");

			LocalDate minDate= LocalDate.of(Integer.parseInt(minDates[0]), Integer.parseInt(minDates[1]),
					Integer.parseInt(minDates[2]));

			LocalDate maxDate= LocalDate.of(Integer.parseInt(maxDates[0]), Integer.parseInt(maxDates[1]),
					Integer.parseInt(maxDates[2]));

			long daysBetween = ChronoUnit.DAYS.between(minDate, maxDate);

			long range=daysBetween/10;
			
			
			
			ranges[level][0]=new Range(	minDate,minDate.plusDays(range));
			for (int i=1;i<9;i++) {
				LocalDate m=(LocalDate)ranges[level][i-1].max;
				ranges[level][i]=new Range(m, m.plusDays(range));
			}
			ranges[level][9]=new Range(ranges[level][8].max,maxDate);
			
			
			for(int i=0;i<10;i++) {
				
				String[] oldMin=((LocalDate)ranges[level][i].min).toString().split("-");
				String[] oldMax=((LocalDate)ranges[level][i].max).toString().split("-");
				
				Date newMin= new Date(Integer.parseInt(oldMin[0])-1900,Integer.parseInt(oldMin[1])-1,Integer.parseInt(oldMin[2]));
				Date newMax= new Date(Integer.parseInt(oldMax[0])-1900,Integer.parseInt(oldMax[1])-1,Integer.parseInt(oldMax[2]));
				
				ranges[level][i]=new Range(newMin,newMax);
			}

		}else {
			String standard="";
			for(int i=0;i<Math.max(min.length(), max.length());i++) {
				if(i<min.length()&&min.charAt(i)==max.charAt(i)) {
					standard+=min.charAt(i);
				}else if(i<min.length()&&min.charAt(i)!=max.charAt(i)) {
					int dif=max.charAt(i)-min.charAt(i);
					int range=dif/10;

					ranges[level][0]=new Range(min,standard+(char)(min.charAt(i)+range));
					range+=dif/10;
					for(int j=1;j<9;j++) {
						ranges[level][j]=new Range(ranges[level][j-1].max,standard+(char)(min.charAt(i)+range));
						range+=dif/10;
					}
					ranges[level][9]=new Range(ranges[level][8].max,max);
					break;
				}else if(i==min.length()) {
					int dif=max.charAt(i);
					int range=dif/10;

					ranges[level][0]=new Range(min,standard+(char)(min.charAt(i)+range));
					range+=dif/10;
					for(int j=1;j<9;j++) {
						ranges[level][j]=new Range(ranges[level][j-1].max,standard+(char)(min.charAt(i)+range));
						range+=dif/10;
					}
					ranges[level][9]=new Range(ranges[level][8].max,max);
					break;
				}
			}
		}





	}


	public void populateIndex(){
		Set<String> indexColumnsSet = this.namesAndLevels.keySet();
		ArrayList<String> indexColumnsArray = new ArrayList<String>(indexColumnsSet.size());
		indexColumnsArray.addAll(indexColumnsSet);
		//If the index has a non-clustering key, we have to loop over every record on the table
		// and see where each record belongs in the index using binary search over the ranges

		//if(this.namesAndLevels.size()>1){  	//If we have more than one column then the index is secondary

			//We will deserialize every page of the table starting from the first page
			int numberOfPages = DBApp.countNumberOfPagesWithoutOverflows(this.tableName);

			//To loop on pages
			for(int i=1 ; i<=numberOfPages ;i++){
				int numberOfOverFlows = DBApp.countNumberOfPageOverflows(this.tableName,i);
				//To loop on their overflows
				for(int j=0; j<=numberOfOverFlows; j++){
					Vector<Hashtable<String, Object>> page = DBApp.readPageIntoVector(this.tableName+"["+i+"]("+j+").class");

					//To loop inside the page
					for(int k=0; k<page.size(); k++){
						insertIntoGrid(page.get(k), this.tableName+"["+i+"]("+j+").class");
					}
				}
			}
		//}
	}

	public void insertIntoGrid(Hashtable<String,Object> row,String pageName){

		Set<String> indexColumnsSet = this.namesAndLevels.keySet();
		ArrayList<String> indexColumnsArray = new ArrayList<String>(indexColumnsSet.size());
		indexColumnsArray.addAll(indexColumnsSet);


		//To get a hashtable of values of needed columns (columns of the index)
		Hashtable<String,Object> colNameAndValue = new Hashtable<String,Object>();
		for(int i=0; i<indexColumnsArray.size(); i++){
			Object value = row.get(indexColumnsArray.get(i));
			colNameAndValue.put(indexColumnsArray.get(i),value);
		}

		//To get value of priamarykey
		Object primaryKeyValue = row.get(this.primaryColumn);


		//After getting this hashtable we will need to use it to know where this row should belong in the grid
		Hashtable<Integer,Integer> levelpositions = getPositionInGrid(colNameAndValue);
		// Now we have to insert the record in this place
		int noOfLevels = indexColumnsArray.size();
		Object[] currentarray = this.array;
		for(int i=0; i<noOfLevels-1; i++){
			currentarray=(Object[])currentarray[levelpositions.get(i)];
		}

		if(currentarray[levelpositions.get(noOfLevels-1)]==null){
			//Create new bucket
			Vector<BucketItem> bucket = new Vector<BucketItem>();
			//Insert in it the first value
			bucket.add(new BucketItem(colNameAndValue,pageName,primaryKeyValue));
			
			//Create its File name
			String bucketFileName="B"+this.tableName;
			for(int i=0;i<noOfLevels;i++) {
				bucketFileName= bucketFileName + "["+levelpositions.get(i) +"]";
			}

			//Serialize it
			try {
				FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\indicesAndBuckets\\"+bucketFileName+ ".class");
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(bucket);
				out.close();
				fileOut.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			//save its name in the grid array
			currentarray[levelpositions.get(noOfLevels-1)] = bucketFileName;
			
		}else{
			// the bucket already exists so insert into it or the first empty overflow
			Vector<BucketItem> bucket = readBucketIntoVector(currentarray[levelpositions.get(noOfLevels-1)].toString()+ ".class");
			bucket.add(new BucketItem(colNameAndValue,pageName,primaryKeyValue));
			//Serialize it
			try {
				FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\indicesAndBuckets\\"+currentarray[levelpositions.get(noOfLevels-1)].toString()+ ".class");
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(bucket);
				out.close();
				fileOut.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

//	public static int getNumberOfBucketOverflows(String bucketName){
//		File dir = new File("src\\main\\resources\\indicesAndBuckets");
//		File[] directoryListing = dir.listFiles();
//		int counter = 0;
//		if (directoryListing != null) {
//			for (File page : directoryListing) {
//				int overflowNumber= DBApp.getFileOverflowNumber(bucketName);
//
//				String currentpageName = "";
//				int c = 0;
//				while (page.getName().charAt(c) != '[') {
//					currentpageName += page.getName().charAt(c);
//					c++;
//				}
//
//				if (&& overflowNumber != 0) {
//					counter++;
//				}
//			}
//		}
//		return counter;
//	}




	public static Vector<BucketItem> readBucketIntoVector(String pageName) {
		String path = "src\\main\\resources\\indicesAndBuckets\\" + pageName;
		Vector<BucketItem> v = null;
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
		return v;
	}


	public Hashtable<Integer,Integer> getPositionInGrid(Hashtable<String,Object> colNameAndValue){

		Hashtable<Integer,Integer> colLevelAndDivision = new Hashtable<Integer,Integer>();
		Set<String> indexColumnsSet = this.namesAndLevels.keySet();
		ArrayList<String> indexColumnsArray = new ArrayList<String>(indexColumnsSet.size());
		indexColumnsArray.addAll(indexColumnsSet);

		for(int i=0; i<indexColumnsArray.size(); i++){
			int level = namesAndLevels.get(indexColumnsArray.get(i));
			Object value = colNameAndValue.get(indexColumnsArray.get(i));
			String dataType = namesAndDataTypes.get(indexColumnsArray.get(i));
			//Now we need to get the position of this value in its level
			if(value==null){
				colLevelAndDivision.put(level,10);
			}else{
				//We will binary search over the divisions
				int start=0;
				int end=9;
				int mid=0;

				while(start<=end){

					mid = (start + end)/2;

					Object minOfDivision = ranges[level][mid].min;
					Object maxOfDivision = ranges[level][mid].max;

					int comparisonWithMin = DBApp.compare(value, minOfDivision, dataType);
					int comparisonWithMax = DBApp.compare(value, maxOfDivision, dataType);
					if(comparisonWithMin<0){
						end = mid-1;
					}else{  // >=0

						if(comparisonWithMax<0){
							colLevelAndDivision.put(level,mid);
							break;
						}else{
							start= mid+1;
						}
					}
				}
				//colLevelAndDivision.put(level,mid);
			}
		}
		return colLevelAndDivision;
	}
}
