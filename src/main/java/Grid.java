import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Hashtable;

public class Grid {
	private String tableName;
	private Object[] array;
	private Range[][] ranges;
	private Hashtable<String,Integer> namesAndLevels;
	
	public Grid(String tableName,String[] strarrColName) {
		this.tableName=tableName;
		for(int i=0;i<strarrColName.length;i++) {
			namesAndLevels.put(strarrColName[i], i);
		}
		array = new Object[10];
		goDeeper(array,strarrColName.length);
		
		ranges = new Range[strarrColName.length][10];
	
		
	}
	
	
	public void goDeeper(Object[] array,int n) {
		if(n==0) {
//			for(int i=0;i<10;i++) {
//				array[i]=null;
//			}
			return;
		}
		
		for(int i=0;i<10;i++) {
			array[i]=new Object[10];
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
					range+=range;
					for(int j=0;j<9;j++) {
						ranges[level][j]=new Range(ranges[level][10].max,standard+(char)(min.charAt(i)+range));
					}
					ranges[level][9]=new Range(ranges[level][8].max,max);
				}else if(i==min.length()) {
					int dif=max.charAt(i);
					int range=dif/10;
					
					ranges[level][0]=new Range(min,standard+(char)(min.charAt(i)+range));
					range+=range;
					for(int j=0;j<9;j++) {
						ranges[level][j]=new Range(ranges[level][10].max,standard+(char)(min.charAt(i)+range));
					}
					ranges[level][9]=new Range(ranges[level][8].max,max);
				}
			}
		}
		
		
		
		
		
	}
}
