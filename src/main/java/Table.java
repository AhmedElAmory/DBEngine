import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class Table {
	private String strTableName;
	private String strClusteringKeyColumn;
	private Hashtable<String, String> htblColNameType;
	private Hashtable<String, String> htblColNameMin;
	private Hashtable<String, String> htblColNameMax;
	
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) {
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		
		try {
			FileWriter csvWriter = new FileWriter("src\\main\\resources\\metadata.csv",true);
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

	public String getStrTableName() {
		return strTableName;
	}

	public String getStrClusteringKeyColumn() {
		return strClusteringKeyColumn;
	}

	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}

	public Hashtable<String, String> getHtblColNameMin() {
		return htblColNameMin;
	}

	public Hashtable<String, String> getHtblColNameMax() {
		return htblColNameMax;
	}

	
}
