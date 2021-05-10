import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Grid {
	private String tableName;
	private Object[] array;
	private Range[][] ranges;
	private Hashtable<String,Integer> namesAndLevels;
	private Hashtable<String,String> namesAndDataTypes;
	
	public Grid(String tableName,String[] strarrColName) {
		this.tableName=tableName;
		for(int i=0;i<strarrColName.length;i++) {
			namesAndLevels.put(strarrColName[i], i);
		}
		array = new Object[11];
		goDeeper(array,strarrColName.length);
		
		ranges = new Range[strarrColName.length][10];
	}

	public void goDeeper(Object[] array,int n) {
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
				BufferedReader csvReader =csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
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

						//To get a hashtable of values of needed columns
						Hashtable<String,Object> colNameAndValue = new Hashtable<String,Object>();
						for(int z=0; z<indexColumnsArray.size();z++){
							Object value = page.get(k).get(indexColumnsArray.get(z));
							colNameAndValue.put(indexColumnsArray.get(z),value);
						}
						//After getting this hashtable we will need to use it to know where this row belongs in the grid
						String positionInGrid = getPositionInGrid(colNameAndValue);
						// Now we have to insert the record in this place

					}
				}
			}
		//}
	}

	public String getPositionInGrid(Hashtable<String,Object> colNameAndValue){

		Set<String> indexColumnsSet = this.namesAndLevels.keySet();
		ArrayList<String> indexColumnsArray = new ArrayList<String>(indexColumnsSet.size());
		indexColumnsArray.addAll(indexColumnsSet);

		for(int i=0; i<indexColumnsArray.size(); i++){

			int level = namesAndLevels.get(indexColumnsArray.get(i));
			Object value = colNameAndValue.get(indexColumnsArray.get(i));
			//Now we need to get the position of this value in its level
			int position;
			if(value==null){
				position=10;
			}else{

				//We will binary search over the divisions
				int start=0;
				int end=9;

				while(start <= end){
					int mid = (start + end)/2;

					Object minOfDivision = ranges[level][mid].min;
					Object maxOfDivision = ranges[level][mid].max;

					int comparison;

				}
			}
		}
		return "";
	}
}
