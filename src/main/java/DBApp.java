import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import org.junit.runner.Result;

public class DBApp implements DBAppInterface {

	Hashtable<String, Object> allIndexes;

	// Constructor
	public DBApp() {
		allIndexes = new Hashtable<String, Object>();
		allIndexes.put("numberOfIndices", -1);
		init();
	}

	public void init() {
		// creating the data folder if doesn't exist
		if (!new File("src\\main\\resources\\data").exists()) {
			// Creating a File object
			File file = new File("src\\main\\resources\\data");
			// Creating the directory
			boolean bool = file.mkdir();
		}
		if (!new File("src\\main\\resources\\indicesAndBuckets").exists()) {
			File file = new File("src\\main\\resources\\indicesAndBuckets");
			boolean bool = file.mkdir();
		}

		if (!new File("src\\main\\resources\\allGrids").exists()) {
			File file = new File("src\\main\\resources\\allGrids");
			boolean bool = file.mkdir();
		}

		// Read indices into memory if exists
		String path = "src\\main\\resources\\allGrids\\indices.class";
		if (new File(path).exists()) {
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

		checkCreateTableExceptions(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
				htblColNameMax);
		try {
			// Reading the metadata file
			FileWriter csvWriter = new FileWriter("src\\main\\resources\\metadata.csv", true);
			// Looping over the Hashtable
			Set<String> keys = htblColNameType.keySet();
			for (String key : keys) {
				boolean cluster = false;
				if (strClusteringKeyColumn.equals(key)) {
					cluster = true;
				}
				csvWriter.append("\n" + strTableName + "," + key + "," + htblColNameType.get(key) + "," + cluster + ","
						+ false + "," + htblColNameMin.get(key) + "," + htblColNameMax.get(key));
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
		checkInsertConstraints(strTableName, htblColNameValue);

		File dir = new File("src\\main\\resources\\data");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			int numberOfpages;
			numberOfpages = countNumberOfPagesWithoutOverflows(strTableName);

			if (numberOfpages == 0) {
				Vector<Hashtable<String, Object>> newPage = new Vector();
				newPage.add(htblColNameValue);
				try {
					FileOutputStream fileOut = new FileOutputStream(
							"src\\main\\resources\\data\\" + strTableName + "[1](0).class");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(newPage);
					out.close();
					fileOut.close();
				} catch (IOException i) {
					i.printStackTrace();
				}

				// now insert in all the indices using insertIntoGrid with row and file
				insertRecordInAllIndices(strTableName + "[1](0).class", htblColNameValue, strTableName);
			} else {
				String primaryKeyCol = "";
				String primarykeyType = "";
				boolean primaryKeyIndexed = false;
				try {
					BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
					String current = br.readLine();
					// check if column names in hashtable exist in metadata
					while (current != null) {
						String arr[] = current.split(",");
						// check if metadata row has same table name as input
						if (arr[0].equals(strTableName)) {
							// check if hashtable contains same column name as metadata
							if (arr[3].equals("true")) {
								primaryKeyCol = arr[1];
								primarykeyType = arr[2];
								if (arr[4].equals("true")) {
									primaryKeyIndexed = true;
								}
							}
						}
						current = br.readLine();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				Object primarykeyvalue = htblColNameValue.get(primaryKeyCol);

				if (primaryKeyIndexed) {
					ArrayList<Grid> allGridsOfTable = (ArrayList<Grid>) allIndexes.get(strTableName);

					for (int i = 0; i < allGridsOfTable.size(); i++) {
						if (allGridsOfTable.get(i).namesAndLevels.size() == 1
								&& allGridsOfTable.get(i).namesAndLevels.get(primaryKeyCol) != null) {
							// We have found an index having only the clustering key and no other columns
							// so we will use it instead of binary search
							String file = lookupInsertPageUsingPrimaryIndex(allGridsOfTable.get(i), primarykeyvalue);
							String fileInsertedTo;
							if (file.equals("-1")) {// index cannot be used so use binary search
								file = binarySearchOnPages(strTableName, numberOfpages, primaryKeyCol, primarykeyvalue,
										primarykeyType);
								fileInsertedTo = binarySearchAndInsertInPages(primaryKeyCol, strTableName,
										getFilePageNumber(file), getFileOverflowNumber(file), primarykeyvalue,
										primarykeyType, htblColNameValue);
							} else {
								fileInsertedTo = binarySearchAndInsertInPages(primaryKeyCol, strTableName,
										getFilePageNumber(file), getFileOverflowNumber(file), primarykeyvalue,
										primarykeyType, htblColNameValue);
								System.out.println("index used to insert");
							}

							// now insert in all the indices using insertIntoGrid with row and file
							insertRecordInAllIndices(fileInsertedTo, htblColNameValue, strTableName);
							return;
						}
					}
				}

				// normal(without index) insert with binary search
				String file = binarySearchOnPages(strTableName, numberOfpages, primaryKeyCol, primarykeyvalue,
						primarykeyType);
				String fileInsertedTo = binarySearchAndInsertInPages(primaryKeyCol, strTableName,
						getFilePageNumber(file), getFileOverflowNumber(file), primarykeyvalue, primarykeyType,
						htblColNameValue);

				// now insert in all the indices using insertIntoGrid with row and file
				insertRecordInAllIndices(fileInsertedTo, htblColNameValue, strTableName);
			}
		}
	}

	// This method will get take as input an index made on only the primary key and
	// returns a position
	// to insert a new row in the table
	public static String lookupInsertPageUsingPrimaryIndex(Grid index, Object clusteringKeyValue)
			throws DBAppException {

		// Create a hashtable having only the key because this is the input that the
		// method getPositionInGrid takes
		Hashtable<String, Object> hashtableWithKey = new Hashtable<String, Object>();
		hashtableWithKey.put(index.primaryColumn, clusteringKeyValue);
		// Get the position(bucket) that the key should be in
		int position = index.getPositionInGrid(hashtableWithKey).get(0);
		// Get the bucket
		String bucketName = (String) index.array[position];

		// Get overflows
		int numberOfOverflows = Grid.getLastOverflowNumber(bucketName);
		// We need to get two rows where the new key should be in between
		Object valueofRowBelow = index.ranges[0][position].max; // asghar rakam akbar meny
		Object valueofRowAbove = index.ranges[0][position].min; // akbar rakam asghar meny
		BucketItem rowBelow = null;
		BucketItem rowAbove = null;

		// Loop on overflows
		for (int j = 0; j <= numberOfOverflows; j++) {
			Vector<BucketItem> bucket = Grid.readBucketIntoVector(bucketName + "(" + j + ").class");

			for (int i = 0; i < bucket.size(); i++) {

				int comparison = compare(clusteringKeyValue, bucket.get(i).primaryKeyValue, index.primaryDataType);
				if (comparison == 0) {
					throw new DBAppException("Primary key already exists!"); // We should not insert if the value exists
																				// already
				} else if (comparison < 0) { // If our key is smaller than the key inside (it should be inserted above
												// it)
					int comparisonAgain = compare(valueofRowBelow, bucket.get(i).primaryKeyValue,
							index.primaryDataType);
					if (comparisonAgain > 0) { // If it is smaller than current valueBelow
						valueofRowBelow = bucket.get(i).primaryKeyValue;
						rowBelow = bucket.get(i);
					}
				} else if (comparison > 0) { // If our key is greater than the key inside
					int comparisonAgain = compare(valueofRowAbove, bucket.get(i).primaryKeyValue,
							index.primaryDataType);
					if (comparisonAgain < 0) { // If it greater than current valueAbove
						valueofRowAbove = bucket.get(i).primaryKeyValue;
						rowAbove = bucket.get(i);
					}
				}
			}
		} // By the end of this loop we should have the rows in either rowBelow or
			// rowAbove or both
		if (rowAbove != null) {
			return rowAbove.pageName;
		} else if (rowBelow != null) {
			return rowBelow.pageName;
		}
		return "-1"; // will be returned if bucket is empty
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the rows to update.
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		checkUpdateConstraints(strTableName, strClusteringKeyValue, htblColNameValue);

		String current;
		Object clusteringKeyValue = "";
		String clusteringKeyType = "";
		String clusteringKeyColumnName = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			current = br.readLine();
			while (current != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(strTableName)) {
					if (arr[3].equals("true")) {
						if (arr[2].equals("java.lang.Integer")) {
							try {
								clusteringKeyValue = Integer.parseInt(strClusteringKeyValue);
							} catch (Exception e) {
								throw new DBAppException("The inserted primary key value is of wrong type");
							}
						} else if (arr[2].equals("java.lang.Double")) {
							try {
								clusteringKeyValue = Double.parseDouble(strClusteringKeyValue);
							} catch (Exception e) {
								throw new DBAppException("The inserted primary key value is of wrong type");
							}
						} else if (arr[2].equals("java.util.Date")) {
							try {
								SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
								try {
									format.parse(strClusteringKeyValue);
								} catch (ParseException e) {
									throw new DBAppException(
											"The primary key has wrong date format make sure it's (YYYY-MM-DD)");
								}
								String primaryKeyDates[] = strClusteringKeyValue.split("-");
								clusteringKeyValue = new Date(Integer.parseInt(primaryKeyDates[0]),
										Integer.parseInt(primaryKeyDates[1]), Integer.parseInt(primaryKeyDates[2]));
							} catch (Exception e) {
								throw new DBAppException("The inserted primary key value is of wrong type");
							}
						} else {
							clusteringKeyValue = strClusteringKeyValue;
						}
						clusteringKeyType = arr[2];
						clusteringKeyColumnName = arr[1];
					}
				}
				current = br.readLine();
			}
			br.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		if (selectSuitableGrid(strTableName, htblColNameValue, true, strClusteringKeyValue) == null) {
			String location = getPageAndIndex(strTableName, clusteringKeyValue, clusteringKeyColumnName,
					clusteringKeyType);
			if (!location.equals("")) {
				String locations[] = location.split(" ");
				String pageFileName = locations[0];
				int index = Integer.parseInt(locations[1]);
				Vector<Hashtable<String, Object>> pageVector = readPageIntoVector(pageFileName);
				pageVector.get(index).putAll(htblColNameValue);
				try {
					FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\" + pageFileName);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(pageVector);
					out.close();
					fileOut.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			Hashtable<String, Object> primaryOnly = new Hashtable<>();
			primaryOnly.put(clusteringKeyColumnName, clusteringKeyValue);
			BucketItem item = deleteRowFromIndexes(strTableName, primaryOnly, clusteringKeyColumnName,
					clusteringKeyType);
			String pageName = item.pageName;
			// binary search through page and get the bucket item

			Vector<Hashtable<String, Object>> pageVector = readPageIntoVector(pageName);
			int start = 0;
			int end = pageVector.size() - 1;

			// Binary Search to get the row
			while (start <= end) {
				int mid = (start + end) / 2;
				int comparison = compare(pageVector.get(mid).get(clusteringKeyColumnName),
						htblColNameValue.get(clusteringKeyColumnName), clusteringKeyType);
				if (comparison == 0) {
					// update the mid
					pageVector.get(mid).putAll(htblColNameValue);
					// serialize the page
					try {
						FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\" + pageName);
						ObjectOutputStream out = new ObjectOutputStream(fileOut);
						out.writeObject(pageVector);
						out.close();
						fileOut.close();
					} catch (IOException e) {
						e.printStackTrace();
					}

					insertRecordInAllIndices(pageName, pageVector.get(mid), strTableName);
				} else if (comparison > 0) {
					end = mid - 1;
				} else {
					start = mid + 1;
				}
			}

		}

	}

	public BucketItem deleteRowFromIndexes(String strTableName, Hashtable<String, Object> removed, String Primarykey,
			String PrimarykeyType) {
		Primarykey=getPrimaryKeyName(strTableName);
		PrimarykeyType=getPrimaryKeyDataType(strTableName);
		if (!allIndexes.containsKey(strTableName)) {
			return null;
		}

		ArrayList<Grid> alltableidexes = (ArrayList<Grid>) allIndexes.get(strTableName);
		BucketItem deleted = null;
		for (Grid grid : alltableidexes) {
			Set<String> indexColumnsSet = grid.namesAndLevels.keySet();
			Hashtable<String, Object> valuesToUseInIndex = new Hashtable<>();
			for (String colNam : indexColumnsSet) {
				if (removed.get(colNam) != null)
					valuesToUseInIndex.put(colNam, removed.get(colNam));
			}
			Hashtable<Integer, Integer> indexPostion = grid.getPositionInGrid(valuesToUseInIndex);
			System.out.println(indexPostion.toString());
			// System.out.println(indexPostion.toString());
			int noOfLevels = indexColumnsSet.size();
			Object[] currentarray = grid.array;
			for (int a = 0; a < noOfLevels - 1; a++) {
				currentarray = (Object[]) currentarray[indexPostion.get(a)];
			}
			String bucket = (String) currentarray[indexPostion.get(noOfLevels - 1)];
			// System.out.println(bucket);
			// delete from buckets and overflow buckets
			if (bucket != null) {
				int numberOfbuckets = getbucketOverflowNumber(bucket);
				// System.out.println(numberOfbuckets);
				for (int n = 0; n <= numberOfbuckets; n++) {
					String bucketName = "B" + grid.tableName + "{" + grid.indexId + "}";
					for (int z = 0; z < noOfLevels; z++) {
						bucketName = bucketName + "[" + indexPostion.get(z) + "]";
					}
					bucketName = bucketName + "(" + n + ").class";
					// System.out.println(bucketName);
					Vector<BucketItem> bucketVector = grid.readBucketIntoVector(bucketName);
					for (int q = 0; q < bucketVector.size(); q++) {
						BucketItem item = bucketVector.get(q);
//						System.out.println(Primarykey);
//						System.out.println(item.primaryKeyValue+" "+removed.get(Primarykey));
//						System.out.println(compare(item.primaryKeyValue, removed.get(Primarykey), PrimarykeyType)==0);
						if ((compare(item.primaryKeyValue, removed.get(Primarykey), PrimarykeyType)) == 0) {
							deleted = bucketVector.remove(q);

						}
					}
					try {
						FileOutputStream fileOut = new FileOutputStream(
								"src\\main\\resources\\indicesAndBuckets\\" + bucketName);
						ObjectOutputStream out = new ObjectOutputStream(fileOut);
						out.writeObject(bucketVector);
						out.close();
						fileOut.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					if (numberOfbuckets > 0 && bucketVector.size() == 0) {
						File filetobedeleted = new File("src\\main\\resources\\indicesAndBuckets\\" + bucketName);
						filetobedeleted.delete();
						String bucketTobeShifted = "B" + grid.tableName + "{" + grid.indexId + "}";
						for (int z = 0; z < noOfLevels; z++) {
							bucketTobeShifted = bucketTobeShifted + "[" + indexPostion.get(z) + "]";
						}
						for (int y = n + 1; y <= numberOfbuckets; y++) {
							File oldfile = new File("src\\main\\resources\\indicesAndBuckets\\" + bucketTobeShifted
									+ "(" + y + ").class");
							File newfile = new File("src\\main\\resources\\indicesAndBuckets\\" + bucketTobeShifted
									+ "(" + (y - 1) + ").class");
							oldfile.renameTo(newfile);
						}
					}
				}
			}
		}
		return deleted;
	}
	
	public Grid selectSuitableGrid(String tableName, Hashtable<String, Object> htblColNameValue,

			boolean primaryKeyExists, String primaryKeyColName) {
		if (!allIndexes.containsKey(tableName)) {
			return null;
		}
		ArrayList<Grid> tableGrids = (ArrayList<Grid>) allIndexes.get(tableName);
		Grid bestGrid = null;
		int countMatches = 0;
		int bestcountMatches = 0;
		int bestsize = 0;
		for (Grid grid : tableGrids) {
			boolean primarykeyflag = false;
			for (String colName : grid.namesAndLevels.keySet()) {
				if (htblColNameValue.containsKey(colName)) {
					countMatches++;
					if (colName.equals(primaryKeyColName)) {
						primarykeyflag = true;
					}
				}
			}
			if (!primaryKeyExists && (countMatches > bestcountMatches || (countMatches != 0
					&& countMatches == bestcountMatches && grid.namesAndLevels.size() < bestsize))) {
				bestGrid = grid;
				bestcountMatches = countMatches;
				bestsize = grid.namesAndLevels.size();
			} else if (primaryKeyExists && countMatches == grid.namesAndLevels.size()) {
				if (primarykeyflag) {
					return grid;
				} else {
					bestGrid = grid;
					bestcountMatches = countMatches;
					bestsize = grid.namesAndLevels.size();
				}
			}
			countMatches = 0;
		}
		return bestGrid;
	}

	public int getbucketOverflowNumber(String bucketFileName) {
		int w = 0;
		String bname = "";
		while (bucketFileName.charAt(w) != '(') {
			bname = bname + bucketFileName.charAt(w);
			w++;
		}

		File dir = new File("src\\main\\resources\\indicesAndBuckets");
		File[] directoryListing = dir.listFiles();
		int counter = -1;
		if (directoryListing != null) {
			for (File page : directoryListing) {

				String bucketName = "";
				int c = 0;
				while (page.getName().charAt(c) != '(') {
					bucketName = bucketName + page.getName().charAt(c);
					c++;
				}
				if (bucketName.equals(bname)) {
					counter++;
				}
			}
		}
		return counter;
	}

	public void shiftingforPageswithoverflows(Object[] array, int n, int overflownumber, int pagenumber,
			int totaloverflownumber, String tablename) {
		if (n == 0) {
//			count++;
//			System.out.println(count);
			for (int i = 0; i < 11; i++) {
				String bucketoriginalName = (String) array[i];
				// System.out.println(bucketoriginalName);
				if (bucketoriginalName != null) {
					int bucketoverflows = getbucketOverflowNumber(bucketoriginalName);
					// System.out.println(bucketoriginalName+"jjjjjj");
					for (int j = 0; j <= bucketoverflows; j++) {
						String bucketName = "";
						int c = 0;
						while (bucketoriginalName.charAt(c) != '(') {
							bucketName = bucketName + bucketoriginalName.charAt(c);
							c++;
						}
						bucketName = bucketName + "(" + j + ").class";
						Vector<BucketItem> bucket = readBucketIntoVector(bucketName);
						for (BucketItem Bitem : bucket) {
							if (totaloverflownumber == 0) {
								String pageName = Bitem.pageName;
								int bitemPagenumber = getFilePageNumber(pageName);
								int bitemOverflownumber = getFileOverflowNumber(pageName);
								if (bitemPagenumber > pagenumber) {
									Bitem.pageName = tablename + "[" + (bitemPagenumber - 1) + "]("
											+ bitemOverflownumber + ").class";
								}
							} else {
								String pageName = Bitem.pageName;
								int bitemPagenumber = getFilePageNumber(pageName);
								int bitemOverflownumber = getFileOverflowNumber(pageName);
								if (bitemOverflownumber > overflownumber) {
									Bitem.pageName = tablename + "[" + bitemPagenumber + "]("
											+ (bitemOverflownumber - 1) + ").class";
								}
							}
						}
						try {
							FileOutputStream fileOut = new FileOutputStream(
									"src\\main\\resources\\indicesAndBuckets\\" + bucketName);
							ObjectOutputStream out = new ObjectOutputStream(fileOut);
							out.writeObject(bucket);
							out.close();
							fileOut.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				}
			}
		} else {
			for (int i = 0; i < 11; i++) {
				shiftingforPageswithoverflows((Object[]) array[i], n - 1, overflownumber, pagenumber,
						totaloverflownumber, tablename);
			}
		}
	}

	public void shiftBucketPageNames(String removedpage) {
//		System.out.println("*******************");
//		System.out.println(removedpage);
		String tableName = getFileTableName(removedpage);
		int pagenumber = getFilePageNumber(removedpage);
		int overflownumber = getFileOverflowNumber(removedpage);
		int totaloverflownumber = countNumberOfPageOverflows(removedpage, pagenumber);
		for (Grid grid : (ArrayList<Grid>) allIndexes.get(tableName)) {
			// System.out.println(grid.namesAndLevels.size());
			shiftingforPageswithoverflows(grid.array, (grid.namesAndLevels.size()) - 1, overflownumber, pagenumber,
					totaloverflownumber, tableName);
		}
	}

	public void partialQueryDeletion(Grid grid, int n, Object[] array, Hashtable<String, Object> deleteConditions) {
		if (n == grid.namesAndLevels.size() - 1) {
			boolean f = false;
			int index = 0;
			for (String colname : deleteConditions.keySet()) {
				if (grid.namesAndLevels.containsKey(colname) && grid.namesAndLevels.get(colname) == n) {
					int level = n;
					Object value = deleteConditions.get(colname);
					String dataType = grid.namesAndDataTypes.get(colname);
					int start = 0;
					int end = 9;
					int mid = 0;

					while (start <= end) {

						mid = (start + end) / 2;

						Object minOfDivision = grid.ranges[level][mid].min;
						Object maxOfDivision = grid.ranges[level][mid].max;

						int comparisonWithMin = compare(value, minOfDivision, dataType);
						int comparisonWithMax = compare(value, maxOfDivision, dataType);
						if (comparisonWithMin < 0) {
							end = mid - 1;
						} else { // >=0

							if (comparisonWithMax < 0) {
								break;
							} else {
								start = mid + 1;
							}
						}
					}
					index = mid;
					f = true;
					break;
				}
			}
			if (f) {
				String bucketoriginalName = (String) array[index];
				// System.out.println("kkkkk");
				if (bucketoriginalName != null) {
					int bucketoverflows = getbucketOverflowNumber(bucketoriginalName);
					// System.out.println(bucketoverflows);
					for (int v = 0; v <= bucketoverflows; v++) {
						String bucketName = "";
						int c = 0;
						while (bucketoriginalName.charAt(c) != '(') {
							bucketName = bucketName + bucketoriginalName.charAt(c);
							c++;
						}
						bucketName = bucketName + "(" + v + ").class";
						Vector<BucketItem> bucket = readBucketIntoVector(bucketName);
						Hashtable<String, ArrayList<Object>> valuesTodelete = new Hashtable<>();
						for (BucketItem Bitem : bucket) {
							if (valuesTodelete.containsKey(Bitem.pageName)) {
								valuesTodelete.get(Bitem.pageName).add(Bitem.primaryKeyValue);
							} else {
								ArrayList<Object> primarykeyarray = new ArrayList<>();
								primarykeyarray.add(Bitem.primaryKeyValue);
								valuesTodelete.put(Bitem.pageName, primarykeyarray);
							}
						}
//						System.out.println("******    " + bucketName + "  " + bucket.size());
//						System.out.println(valuesTodelete.toString());
						// System.out.println("jjjj");
						for (String pageName : valuesTodelete.keySet()) {
							Vector<Hashtable<String, Object>> pageVector = readPageIntoVector(pageName);
							ArrayList<Object> keys = valuesTodelete.get(pageName);
							// System.out.println("dds");
							for (Object key : keys) {
								int start = 0;
								int end = pageVector.size() - 1;
								// Binary Search to get the row
								while (start <= end) {
									int mid = (start + end) / 2;

									int comparison = compare(key, pageVector.get(mid).get(grid.primaryColumn),
											grid.primaryDataType);
									if (comparison == 0) {
										// System.out.println("kk");
										boolean checkDelete = true;
										Set<String> columns = deleteConditions.keySet();
//										System.out.println(pageVector.get(mid).toString());
										for (String column : columns) {
											if (!(pageVector.get(mid).get(column)
													.equals(deleteConditions.get(column)))) {
												checkDelete = false; // If false then we do not want to delete this row
											}
										}
										// System.out.println("mkk");
										if (checkDelete) {
											String strTableName = grid.tableName;
											Hashtable<String, Object> removed = pageVector.remove(mid);

											// System.out.println("fff");
											deleteRowFromIndexes(strTableName, removed, grid.primaryColumn,
													grid.primaryDataType);
											// System.out.println("fff");
											try {
												FileOutputStream fileOut = new FileOutputStream(
														"src\\main\\resources\\data\\" + strTableName + "["
																+ getFilePageNumber(pageName) + "]("
																+ getFileOverflowNumber(pageName) + ")" + ".class");
												ObjectOutputStream out = new ObjectOutputStream(fileOut);
												out.writeObject(pageVector);
												out.close();
												fileOut.close();
											} catch (IOException e) {
												e.printStackTrace();
											}

											break;
										}
										break;

									} else if (comparison > 0) {
										// System.out.println(start+" "+end);
										start = mid + 1;
									} else {
										end = mid - 1;
									}

								}
							}
							String strTableName = grid.tableName;
							int i = getFilePageNumber(pageName);
							int j = getFileOverflowNumber(pageName);
							int TotalNumberOfPages = countNumberOfPagesWithoutOverflows(strTableName);
							int TotalNumberOfOverflowPages = countNumberOfPageOverflows(strTableName, i);
							// Check if page is empty
							if (j == 0) { // Check if it is not an overflow page
								if (pageVector.size() == 0) { // If the page is empty (size of
																// vector==0), we delete
																// it
									String removedPage = strTableName + "[" + i + "](" + j + ").class";
									shiftBucketPageNames(removedPage);
									File filetobedeleted = new File("src\\main\\resources\\data\\" + strTableName + "["
											+ i + "](" + j + ").class");
									filetobedeleted.delete();

									// Check if the deleted page had any overflows
									if (TotalNumberOfOverflowPages == 0) {
										// If no, we shift all the pages(and their overflows) under it
										// up one step

										for (int x = i + 1; x <= TotalNumberOfPages; x++) {
											int temp = x - 1;
											int TotalNumberOfOverflowPages2 = countNumberOfPageOverflows(strTableName,
													x);
											for (int k = 0; k <= TotalNumberOfOverflowPages2; k++) {
												File oldfile = new File("src\\main\\resources\\data\\" + strTableName
														+ "[" + x + "](" + k + ").class");
												File newfile = new File("src\\main\\resources\\data\\" + strTableName
														+ "[" + temp + "](" + k + ").class");
												oldfile.renameTo(newfile);
											}
										}
									} else {
										// If Yes(it did have overflows), we shift only the overflows to
										// the left
										// one
										// step
										for (int l = 1; l <= TotalNumberOfOverflowPages; l++) {

											File oldfile = new File("src\\main\\resources\\data\\" + strTableName + "["
													+ i + "](" + l + ").class");
											int temp = l - 1;
											File newfile = new File("src\\main\\resources\\data\\" + strTableName + "["
													+ i + "](" + temp + ").class");
											oldfile.renameTo(newfile);
										}
									}
								}
							} else { // it is an overflow page
								if (pageVector.size() == 0) { // If the page is empty (size of
																// vector==0), we delete
																// it
									String removedPage = strTableName + "[" + i + "](" + j + ").class";
									shiftBucketPageNames(removedPage);
									File f1 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i + "]("
											+ j + ").class");
									f1.delete();

									// Decrement all overflow pages with overflow number > j
									for (int x = j + 1; x <= TotalNumberOfOverflowPages; x++) {
										File f2 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
												+ "](" + x + ").class");
										int temp = x - 1;
										File f3 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
												+ "](" + temp + ").class");
										f2.renameTo(f3);
									}
								}
							}
							// System.out.println("vbb");
						}

					}
				}
			} else {
				for (int y = 0; y < 11; y++) {
					String bucketoriginalName = (String) array[y];
					if (bucketoriginalName != null) {
						int bucketoverflows = getbucketOverflowNumber(bucketoriginalName);

						for (int v = 0; v <= bucketoverflows; v++) {
							String bucketName = "";
							int c = 0;
							while (bucketoriginalName.charAt(c) != '(') {
								bucketName = bucketName + bucketoriginalName.charAt(c);
								c++;
							}
							bucketName = bucketName + "(" + v + ").class";
							Vector<BucketItem> bucket = readBucketIntoVector(bucketName);
							Hashtable<String, ArrayList<Object>> valuesTodelete = new Hashtable<>();
							for (BucketItem Bitem : bucket) {
								if (valuesTodelete.containsKey(Bitem.pageName)) {
									valuesTodelete.get(Bitem.pageName).add(Bitem.primaryKeyValue);
								} else {
									ArrayList<Object> primarykeyarray = new ArrayList<>();
									primarykeyarray.add(Bitem.primaryKeyValue);
									valuesTodelete.put(Bitem.pageName, primarykeyarray);
								}
							}
							for (String pageName : valuesTodelete.keySet()) {
								Vector<Hashtable<String, Object>> pageVector = readPageIntoVector(pageName);
								ArrayList<Object> keys = valuesTodelete.get(pageName);
								for (Object key : keys) {
									int start = 0;
									int end = pageVector.size() - 1;
									// Binary Search to get the row
									while (start <= end) {

										int mid = (start + end) / 2;

										int comparison = compare(pageVector.get(mid).get(grid.primaryColumn), key,
												grid.primaryDataType);
										if (comparison == 0) {
											boolean checkDelete = true;
											Set<String> columns = deleteConditions.keySet();
											for (String column : columns) {
												if (!pageVector.get(mid).get(column)
														.equals(deleteConditions.get(column))) {
													checkDelete = false; // If false then we do not want to delete this
																			// row
												}
											}
											if (checkDelete) {
												String strTableName = grid.tableName;
												Hashtable<String, Object> removed = pageVector.remove(mid);
												deleteRowFromIndexes(strTableName, removed, grid.primaryColumn,
														grid.primaryDataType);
												try {
													FileOutputStream fileOut = new FileOutputStream(
															"src\\main\\resources\\data\\" + strTableName + "["
																	+ getFilePageNumber(pageName) + "]("
																	+ getFileOverflowNumber(pageName) + ")" + ".class");
													ObjectOutputStream out = new ObjectOutputStream(fileOut);
													out.writeObject(pageVector);
													out.close();
													fileOut.close();
												} catch (IOException e) {
													e.printStackTrace();
												}
												break;
											}
											break;

										} else if (comparison > 0) {
											end = mid - 1;
										} else {
											start = mid + 1;
										}

									}
								}
								String strTableName = grid.tableName;
								int i = getFilePageNumber(pageName);
								int j = getFileOverflowNumber(pageName);
								int TotalNumberOfPages = countNumberOfPagesWithoutOverflows(strTableName);
								int TotalNumberOfOverflowPages = countNumberOfPageOverflows(strTableName, i);
								// Check if page is empty
								if (j == 0) { // Check if it is not an overflow page
									if (pageVector.size() == 0) { // If the page is empty (size of
																	// vector==0), we delete
																	// it
										String removedPage = strTableName + "[" + i + "](" + j + ").class";
										shiftBucketPageNames(removedPage);
										File filetobedeleted = new File("src\\main\\resources\\data\\" + strTableName
												+ "[" + i + "](" + j + ").class");
										filetobedeleted.delete();

										// Check if the deleted page had any overflows
										if (TotalNumberOfOverflowPages == 0) {
											// If no, we shift all the pages(and their overflows) under it
											// up one step

											for (int x = i + 1; x <= TotalNumberOfPages; x++) {
												int temp = x - 1;
												int TotalNumberOfOverflowPages2 = countNumberOfPageOverflows(
														strTableName, x);
												for (int k = 0; k <= TotalNumberOfOverflowPages2; k++) {
													File oldfile = new File("src\\main\\resources\\data\\"
															+ strTableName + "[" + x + "](" + k + ").class");
													File newfile = new File("src\\main\\resources\\data\\"
															+ strTableName + "[" + temp + "](" + k + ").class");
													oldfile.renameTo(newfile);
												}
											}
										} else {
											// If Yes(it did have overflows), we shift only the overflows to
											// the left
											// one
											// step
											for (int l = 1; l <= TotalNumberOfOverflowPages; l++) {

												File oldfile = new File("src\\main\\resources\\data\\" + strTableName
														+ "[" + i + "](" + l + ").class");
												int temp = l - 1;
												File newfile = new File("src\\main\\resources\\data\\" + strTableName
														+ "[" + i + "](" + temp + ").class");
												oldfile.renameTo(newfile);
											}
										}
									}
								} else { // it is an overflow page
									if (pageVector.size() == 0) { // If the page is empty (size of
																	// vector==0), we delete
																	// it
										String removedPage = strTableName + "[" + i + "](" + j + ").class";
										shiftBucketPageNames(removedPage);
										File f1 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
												+ "](" + j + ").class");
										f1.delete();

										// Decrement all overflow pages with overflow number > j
										for (int x = j + 1; x <= TotalNumberOfOverflowPages; x++) {
											File f2 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
													+ "](" + x + ").class");
											int temp = x - 1;
											File f3 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
													+ "](" + temp + ").class");
											f2.renameTo(f3);
										}
									}
								}
							}
						}
					}
				}
			}
		} else {
			boolean f = false;
			int index = 0;
			// System.out.println(index);
			for (String colname : deleteConditions.keySet()) {
				// System.out.println(index);
				if (grid.namesAndLevels.containsKey(colname) && grid.namesAndLevels.get(colname) == n) {
					int level = n;
					Object value = deleteConditions.get(colname);
					String dataType = grid.namesAndDataTypes.get(colname);
					int start = 0;
					int end = 9;
					int mid = 0;

					while (start <= end) {
						// System.out.println(index);
						mid = (start + end) / 2;

						Object minOfDivision = grid.ranges[level][mid].min;
						Object maxOfDivision = grid.ranges[level][mid].max;

						int comparisonWithMin = compare(value, minOfDivision, dataType);
						int comparisonWithMax = compare(value, maxOfDivision, dataType);
						if (comparisonWithMin < 0) {
							end = mid - 1;
						} else { // >=0

							if (comparisonWithMax < 0) {
								break;
							} else {
								start = mid + 1;
							}
						}
					}
					index = mid;
					f = true;
					break;
				}
			}
			if (f) {
				// System.out.println(n);
				partialQueryDeletion(grid, n + 1, (Object[]) array[index], deleteConditions);
			} else {
				// System.out.println(index);
				for (int o = 0; o < 11; o++) {
//					count++;
//					System.out.println(count);
					partialQueryDeletion(grid, n + 1, (Object[]) array[o], deleteConditions);
				}
			}
		}

	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue entries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		// Check constraints
		String PrimaryKey = checkDeleteConstraints(strTableName, htblColNameValue);

		// (M2) First check for usable indices
		// Check if PrimaryKey exists
		if (PrimaryKey.equals(" ")) {
			// Do linear search
			Grid chosenIndex = selectSuitableGrid(strTableName, htblColNameValue, false, "");
			if (chosenIndex == null) {
				linearDeleteOnTable(strTableName, htblColNameValue);
			} else {
				partialQueryDeletion(chosenIndex, 0, chosenIndex.array, htblColNameValue);
			}
		} else {
			// If primarykey exists then we will delete only one unique row so we can use
			// binary search as the column is sorted
			// Do binary search
			String[] primarykeyData = PrimaryKey.split(" ");
			Grid chosenIndex = selectSuitableGrid(strTableName, htblColNameValue, true, primarykeyData[0]);
			if (chosenIndex == null) {
				String pageToDeleteFrom = binaryDeleteFromTable(strTableName, htblColNameValue, primarykeyData[0],
						primarykeyData[1]);

				if (!(pageToDeleteFrom.equals(""))) {
					Vector<Hashtable<String, Object>> pageVector = readPageIntoVector(pageToDeleteFrom);
					int start = 0;
					int end = pageVector.size() - 1;

					// Binary Search to get the row
					while (start <= end) {
						int mid = (start + end) / 2;

						int comparison = compare(pageVector.get(mid).get(primarykeyData[0]),
								htblColNameValue.get(primarykeyData[0]), primarykeyData[1]);
						if (comparison == 0) {
							boolean checkDelete = true;
							Set<String> columns = htblColNameValue.keySet();
							for (String column : columns) {
								if (!pageVector.get(mid).get(column).equals(htblColNameValue.get(column))) {
									checkDelete = false; // If false then we do not want to delete this row
								}
							}
							if (checkDelete) {
								Hashtable<String, Object> removed = pageVector.remove(mid);
								deleteRowFromIndexes(strTableName, removed, primarykeyData[0], primarykeyData[1]);
								try {
									FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\"
											+ strTableName + "[" + getFilePageNumber(pageToDeleteFrom) + "]("
											+ getFileOverflowNumber(pageToDeleteFrom) + ")" + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(pageVector);
									out.close();
									fileOut.close();
								} catch (IOException e) {
									e.printStackTrace();
								}
								int i = getFilePageNumber(pageToDeleteFrom);
								int j = getFileOverflowNumber(pageToDeleteFrom);
								int TotalNumberOfPages = countNumberOfPagesWithoutOverflows(strTableName);
								int TotalNumberOfOverflowPages = countNumberOfPageOverflows(strTableName, i);
								// Check if page is empty
								if (j == 0) { // Check if it is not an overflow page
									if (pageVector.size() == 0) { // If the page is empty (size of vector==0), we delete
																	// it
										String removedPage = strTableName + "[" + i + "](" + j + ").class";
										shiftBucketPageNames(removedPage);
										File filetobedeleted = new File("src\\main\\resources\\data\\" + strTableName
												+ "[" + i + "](" + j + ").class");
										filetobedeleted.delete();

										// Check if the deleted page had any overflows
										if (TotalNumberOfOverflowPages == 0) {
											// If no, we shift all the pages(and their overflows) under it up one step

											for (int x = i + 1; x <= TotalNumberOfPages; x++) {
												int temp = x - 1;
												int TotalNumberOfOverflowPages2 = countNumberOfPageOverflows(
														strTableName, x);
												for (int k = 0; k <= TotalNumberOfOverflowPages2; k++) {
													File oldfile = new File("src\\main\\resources\\data\\"
															+ strTableName + "[" + x + "](" + k + ").class");
													File newfile = new File("src\\main\\resources\\data\\"
															+ strTableName + "[" + temp + "](" + k + ").class");
													oldfile.renameTo(newfile);
												}
											}
										} else {
											// If Yes(it did have overflows), we shift only the overflows to the left
											// one
											// step
											for (int l = 1; l <= TotalNumberOfOverflowPages; l++) {

												File oldfile = new File("src\\main\\resources\\data\\" + strTableName
														+ "[" + i + "](" + l + ").class");
												int temp = l - 1;
												File newfile = new File("src\\main\\resources\\data\\" + strTableName
														+ "[" + i + "](" + temp + ").class");
												oldfile.renameTo(newfile);
											}
										}
									}
								} else { // it is an overflow page
									if (pageVector.size() == 0) { // If the page is empty (size of vector==0), we delete
																	// it
										String removedPage = strTableName + "[" + i + "](" + j + ").class";
										shiftBucketPageNames(removedPage);
										File f1 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
												+ "](" + j + ").class");
										f1.delete();

										// Decrement all overflow pages with overflow number > j
										for (int x = j + 1; x <= TotalNumberOfOverflowPages; x++) {
											File f2 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
													+ "](" + x + ").class");
											int temp = x - 1;
											File f3 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
													+ "](" + temp + ").class");
											f2.renameTo(f3);
										}
									}
								}
								break;
							}
							break;
						} else if (comparison > 0) {
							end = mid - 1;
						} else {
							start = mid + 1;
						}

					}
				} else {
					System.out.println("No rows found");
				}
			} else {
				Hashtable<String, Object> inputHashtableSubset = new Hashtable<String, Object>();
				for (String colName : chosenIndex.namesAndLevels.keySet()) {
					if (htblColNameValue.containsKey(colName)) {
						inputHashtableSubset.put(colName, htblColNameValue.get(colName));
					}
				}
				Hashtable<Integer, Integer> indexPostion = chosenIndex.getPositionInGrid(inputHashtableSubset);
				int noOfLevels = chosenIndex.namesAndLevels.keySet().size();
				Object[] currentarray = chosenIndex.array;
				for (int a = 0; a < noOfLevels - 1; a++) {
					currentarray = (Object[]) currentarray[indexPostion.get(a)];
				}
				String bucketString = (String) currentarray[indexPostion.get(noOfLevels - 1)];
				if (bucketString != null) {
					int numberOfbuckets = getbucketOverflowNumber(bucketString);
					for (int n = 0; n <= numberOfbuckets; n++) {
						String bucketName = "B" + chosenIndex.tableName + "{" + chosenIndex.indexId + "}";
						for (int z = 0; z < noOfLevels; z++) {
							bucketName = bucketName + "[" + indexPostion.get(z) + "]";
						}
						bucketName = bucketName + "(" + n + ").class";
						Vector<BucketItem> bucket = chosenIndex.readBucketIntoVector(bucketName);
						for (BucketItem bItem : bucket) {
							if (compare(bItem.primaryKeyValue, htblColNameValue.get(primarykeyData[0]),
									primarykeyData[1]) == 0) {
								Vector<Hashtable<String, Object>> pageVector = readPageIntoVector(bItem.pageName);
								int start = 0;
								int end = pageVector.size() - 1;
								while (start <= end) {
									int mid = (start + end) / 2;

									int comparison = compare(pageVector.get(mid).get(primarykeyData[0]),
											htblColNameValue.get(primarykeyData[0]), primarykeyData[1]);
									if (comparison == 0) {
										boolean checkDelete = true;
										Set<String> columns = htblColNameValue.keySet();
										for (String column : columns) {
											if (!pageVector.get(mid).get(column).equals(htblColNameValue.get(column))) {
												checkDelete = false; // If false then we do not want to delete this row
											}
										}
										if (checkDelete) {
											Hashtable<String, Object> removed = pageVector.remove(mid);
											deleteRowFromIndexes(strTableName, removed, primarykeyData[0],
													primarykeyData[1]);
											try {
												FileOutputStream fileOut = new FileOutputStream(
														"src\\main\\resources\\data\\" + strTableName + "["
																+ getFilePageNumber(bItem.pageName) + "]("
																+ getFileOverflowNumber(bItem.pageName) + ")"
																+ ".class");
												ObjectOutputStream out = new ObjectOutputStream(fileOut);
												out.writeObject(pageVector);
												out.close();
												fileOut.close();
											} catch (IOException e) {
												e.printStackTrace();
											}
											int i = getFilePageNumber(bItem.pageName);
											int j = getFileOverflowNumber(bItem.pageName);
											int TotalNumberOfPages = countNumberOfPagesWithoutOverflows(strTableName);
											int TotalNumberOfOverflowPages = countNumberOfPageOverflows(strTableName,
													i);
											// Check if page is empty
											if (j == 0) { // Check if it is not an overflow page
												if (pageVector.size() == 0) { // If the page is empty (size of
																				// vector==0), we delete
																				// it
													String removedPage = strTableName + "[" + i + "](" + j + ").class";
													shiftBucketPageNames(removedPage);
													File filetobedeleted = new File("src\\main\\resources\\data\\"
															+ strTableName + "[" + i + "](" + j + ").class");
													filetobedeleted.delete();

													// Check if the deleted page had any overflows
													if (TotalNumberOfOverflowPages == 0) {
														// If no, we shift all the pages(and their overflows) under it
														// up one step

														for (int x = i + 1; x <= TotalNumberOfPages; x++) {
															int temp = x - 1;
															int TotalNumberOfOverflowPages2 = countNumberOfPageOverflows(
																	strTableName, x);
															for (int k = 0; k <= TotalNumberOfOverflowPages2; k++) {
																File oldfile = new File(
																		"src\\main\\resources\\data\\" + strTableName
																				+ "[" + x + "](" + k + ").class");
																File newfile = new File(
																		"src\\main\\resources\\data\\" + strTableName
																				+ "[" + temp + "](" + k + ").class");
																oldfile.renameTo(newfile);
															}
														}
													} else {
														// If Yes(it did have overflows), we shift only the overflows to
														// the left
														// one
														// step
														for (int l = 1; l <= TotalNumberOfOverflowPages; l++) {

															File oldfile = new File("src\\main\\resources\\data\\"
																	+ strTableName + "[" + i + "](" + l + ").class");
															int temp = l - 1;
															File newfile = new File("src\\main\\resources\\data\\"
																	+ strTableName + "[" + i + "](" + temp + ").class");
															oldfile.renameTo(newfile);
														}
													}
												}
											} else { // it is an overflow page
												if (pageVector.size() == 0) { // If the page is empty (size of
																				// vector==0), we delete
																				// it
													String removedPage = strTableName + "[" + i + "](" + j + ").class";
													shiftBucketPageNames(removedPage);
													File f1 = new File("src\\main\\resources\\data\\" + strTableName
															+ "[" + i + "](" + j + ").class");
													f1.delete();

													// Decrement all overflow pages with overflow number > j
													for (int x = j + 1; x <= TotalNumberOfOverflowPages; x++) {
														File f2 = new File("src\\main\\resources\\data\\" + strTableName
																+ "[" + i + "](" + x + ").class");
														int temp = x - 1;
														File f3 = new File("src\\main\\resources\\data\\" + strTableName
																+ "[" + i + "](" + temp + ").class");
														f2.renameTo(f3);
													}
												}
											}
											break;
										}
										break;
									} else if (comparison > 0) {
										end = mid - 1;
									} else {
										start = mid + 1;
									}

								}
							}
						}
					}
				}
			}
		}
	}

	// following method creates one index – either multidimensional
	// or single dimension depending on the count of column names passed.
	// following method creates one index – either multidimensional
	// or single dimension depending on the count of column names passed.
	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {

		String primarycolAndDataType = checkCreateIndexExceptions(strTableName, strarrColName);
		ArrayList<Grid> currentIndicesOfTable = (ArrayList<Grid>) allIndexes.get(strTableName);

		// First check if there exist an index with same columns
		if (currentIndicesOfTable != null) {
			for (int i = 0; i < currentIndicesOfTable.size(); i++) {
				Hashtable<String, Integer> namesAndLevels = currentIndicesOfTable.get(i).namesAndLevels;
				int countOfSameColumns = 0;
				for (int j = 0; j < strarrColName.length; j++) {
					if (namesAndLevels.containsKey(strarrColName[j])) {
						countOfSameColumns++;
					}
				}
				if (namesAndLevels.size() == countOfSameColumns && countOfSameColumns == strarrColName.length) { // i.e
																													// same
																													// index
					throw new DBAppException("This index already exists!");
				}
			}
		}

		String[] x = primarycolAndDataType.split(",");
		String primaryCol = x[0];
		String primaryDataType = x[1];
		int id = (Integer) allIndexes.get("numberOfIndices") + 1; // To be able to identify each index
		Grid newIndex = new Grid(strTableName, strarrColName, primaryCol, primaryDataType, id);

		if (currentIndicesOfTable == null) { // (No indices exist for this table)
			currentIndicesOfTable = new ArrayList<Grid>();
			currentIndicesOfTable.add(newIndex);
			allIndexes.put(strTableName, currentIndicesOfTable);
		} else {
			currentIndicesOfTable.add(newIndex);
		}

		// to update indices count
		allIndexes.put("numberOfIndices", id);

		try {
			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\allGrids\\indices.class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(allIndexes);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// After creating an index we have to edit the metadata to indicate that an
		// index is created on specific columns
		updateCSV(strTableName, strarrColName);

	}

	public static void updateCSV(String strTableName, String[] strarrColName) {

		ArrayList<String> listOfCol = new ArrayList<String>();
		for (int i = 0; i < strarrColName.length; i++) {
			listOfCol.add(strarrColName[i]);
		}
		// Read csv to arraylist of arrays
		ArrayList<String[]> arr = new ArrayList<String[]>();
		BufferedReader csvReader = null;
		try {
			csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String row = csvReader.readLine();
			while (row != null) {
				String[] data = row.split(",");
				arr.add(data);
				row = csvReader.readLine();
			}
			csvReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// loop on arraylist to edit needed rows
		for (int i = 0; i < arr.size(); i++) {
			// check if this is needed row
			if (arr.get(i)[0].equals(strTableName) && listOfCol.contains(arr.get(i)[1])) {
				arr.get(i)[4] = "true";
			}
		}
		// Now write arraylist to csv file again
		try {
			FileWriter csvWriter = new FileWriter("src\\main\\resources\\metadata.csv");
			csvWriter.append(""); // to clear csv file first

			csvWriter = new FileWriter("src\\main\\resources\\metadata.csv", true);
			// Loop over arraylist
			for (int i = 0; i < arr.size(); i++) {
				if (arr.get(i).length == 7) {
					csvWriter.append("\n" + arr.get(i)[0] + "," + arr.get(i)[1] + "," + arr.get(i)[2] + ","
							+ arr.get(i)[3] + "," + arr.get(i)[4] + "," + arr.get(i)[5] + "," + arr.get(i)[6]);
				}
			}
			csvWriter.flush();
			csvWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String checkCreateIndexExceptions(String strTableName, String[] strarrColName) throws DBAppException {

		String primaryColumn = "";
		String primaryDataType = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();

			// check if column names in hashtable exist in metadata
			int countCorrectColumns = 0;
			boolean tableExist = false;
			ArrayList<String> listOfCol = new ArrayList<String>();
			for (int i = 0; i < strarrColName.length; i++) {
				listOfCol.add(strarrColName[i]);
			}

			while (current != null) {
				String arr[] = current.split(",");
				// check if metadata row has same table name as input
				if (arr[0].equals(strTableName)) {
					tableExist = true;
					// check if hashtable contains same column name as metadata
					if (listOfCol.contains(arr[1])) {
						countCorrectColumns++;
					}
					if (arr[3].equals("true")) {
						primaryColumn = arr[1];
						primaryDataType = arr[2];
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
		return primaryColumn + "," + primaryDataType;
	}

	public void checkSelectConstraints(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		boolean flag = true;
		// Check if the length of both arrays is correct
		if (arrSQLTerms.length != strarrOperators.length + 1) {
			throw new DBAppException("The number of operators should be equal to number of SQL terms - 1");
		}
		try {
			int totalFound = 0;
			boolean tableFound = false;
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current;
			// Check if they are all from the same table
			for (int i = 0; i < arrSQLTerms.length; i++) {
				if (arrSQLTerms[i]._strTableName != arrSQLTerms[0]._strTableName) {
					throw new DBAppException("The inserted SQL Terms are not from the same table");
				}
			}
			// Check if they can all be found in the table
			current = br.readLine();
			while (current != null) {
				if (!current.equals("")) {
					String line[] = current.split(",");
					String tableName = line[0];
					String columnName = line[1];
					String dataType = line[2];
					for (int i = 0; i < arrSQLTerms.length; i++) {
						if (arrSQLTerms[i]._strTableName.equals(tableName)) {
							tableFound = true;
						}
						if (arrSQLTerms[i]._strTableName.equals(tableName)
								&& arrSQLTerms[i]._strColumnName.equals(columnName)) {
							// try typecasting the sqlterm value using the type inside the metadata file..
							// if failed then throw exception
							if (dataType.equals("java.lang.Integer")) {
								try {
									int x = (Integer) (arrSQLTerms[i]._objValue);
								} catch (Exception e) {
									throw new DBAppException("The sql term " + arrSQLTerms[i]._strColumnName
											+ " object value is not an integer");
								}
							} else if (dataType.equals("java.lang.Double")) {
								try {
									Double x = (Double) (arrSQLTerms[i]._objValue);
								} catch (Exception e) {
									throw new DBAppException("The sql term " + arrSQLTerms[i]._strColumnName
											+ " object value is not a Double");
								}
							} else if (dataType.equals("java.util.Date")) {
								try {
									Date x = (Date) (arrSQLTerms[i]._objValue);
								} catch (Exception e) {
									throw new DBAppException("The sql term " + arrSQLTerms[i]._strColumnName
											+ " object value is not a Date");
								}
							} else {
								try {
									String text = (String) arrSQLTerms[i]._objValue;
								} catch (Exception e) {
									throw new DBAppException("The sql term " + arrSQLTerms[i]._strColumnName
											+ " object value is not a String");
								}
							}
							totalFound++;
						}
					}
				}
				current = br.readLine();
			}
			// If table notFound throw an exception
			if (!tableFound) {
				throw new DBAppException("The table " + arrSQLTerms[0]._strTableName + " does not exist");
			}

			// If not all the SQL Terms are found in the table then throw an exception
			if (totalFound != arrSQLTerms.length) {
				throw new DBAppException("The inserted SQL Terms do not exist in the specefied table");
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {

		// check select constraints
		checkSelectConstraints(arrSQLTerms, strarrOperators);

		// check if there is any index on the table
		// This code executes if there is and index on the table

		ArrayList<SQLTerm> SQLTermList = new ArrayList<>();
		ArrayList<String> operatorsList = new ArrayList<>();

		for (int i = 0; i < arrSQLTerms.length; i++) {
			SQLTermList.add(arrSQLTerms[i]);
		}
		for (int i = 0; i < strarrOperators.length; i++) {
			operatorsList.add(strarrOperators[i]);
		}

		String tableName1 = SQLTermList.get(0)._strTableName;
		ArrayList<ArrayList<SQLTerm>> ANDedPairs = new ArrayList<>();
		Stack<SQLTerm> stack = new Stack<>();
		for (int i = 0; i < arrSQLTerms.length; i++) {
			stack.push(SQLTermList.get(i));
			if (i == SQLTermList.size() - 1 || !operatorsList.get(i).equals("AND")) {
				ArrayList<SQLTerm> list = new ArrayList<>();
				while (!stack.isEmpty()) {
					list.add(stack.pop());
				}
				ANDedPairs.add(list);
			}
		}

		ArrayList<Grid> Grids = new ArrayList<>();
		Stack<HashSet<Hashtable<String, Object>>> stack2 = new Stack<>();
		if (checkAllPairsHaveGridsAndNoNotEqualOrHavePrimaryKey(ANDedPairs, Grids)) {
			ArrayList<String> collection = new ArrayList<>();
			collection.add("AND");
			operatorsList.removeAll(collection);
			ArrayList<HashSet<Hashtable<String, Object>>> ANDedResult = getANDedPairsResult(ANDedPairs, Grids);
			ArrayList<ArrayList<HashSet<Hashtable<String, Object>>>> ORedPairs = new ArrayList<>();
			for (int i = 0; i < ANDedResult.size(); i++) {
				stack2.push(ANDedResult.get(i));
				if (i == ANDedResult.size() - 1 || operatorsList.size() == 0 || !operatorsList.get(i).equals("OR")) {
					ArrayList<HashSet<Hashtable<String, Object>>> list = new ArrayList<>();
					while (!stack2.isEmpty()) {
						list.add(stack2.pop());
					}
					ORedPairs.add(list);
				}
			}

			ArrayList<HashSet<Hashtable<String, Object>>> XORedPairs = new ArrayList<>();
			for (int i = 0; i < ORedPairs.size(); i++) {
				HashSet<Hashtable<String, Object>> pairResult = new HashSet<>();
				for (int j = 0; j < ORedPairs.get(i).size(); j++) {
					pairResult.addAll(ORedPairs.get(i).get(j));
				}
				XORedPairs.add(pairResult);
			}

			while (XORedPairs.size() > 1) {
				HashSet<Hashtable<String, Object>> Union = new HashSet<>();
				HashSet<Hashtable<String, Object>> Intersection = new HashSet<>();
				Union.addAll(XORedPairs.get(0));
				Union.addAll(XORedPairs.get(1));
				Intersection.addAll(XORedPairs.get(0));
				Intersection.retainAll(XORedPairs.get(1));
				Union.removeAll(Intersection);
				XORedPairs.add(0, Union);
				XORedPairs.remove(1);
				XORedPairs.remove(1);
			}
			HashSet<Hashtable<String, Object>> finalResultSet = XORedPairs.get(0);
			ArrayList<Hashtable<String, Object>> finalResultList = new ArrayList<>();
			finalResultList.addAll(finalResultSet);
			// sort by primary key if needed
			return finalResultList.iterator();
			// return the results...
		} else {
			// Check if all queries have primary key
			// if true then go to sawy's code
			boolean flag = true;
			for (int i = 0; i < ANDedPairs.size(); i++) {
				HashSet<String> hashSetColumnName = new HashSet<>();

				for (int j = 0; j < ANDedPairs.get(i).size(); j++) {
					hashSetColumnName.add(ANDedPairs.get(i).get(j)._strColumnName);
				}
				String pkey = getPrimaryKeyName(tableName1);
				if (!hashSetColumnName.contains(pkey)) {
					flag = false;
					break;
				}

				for (int j = 0; j < ANDedPairs.get(i).size(); j++) {
					if ((ANDedPairs.get(i).get(j)._strOperator.equals("!=")
							&& ANDedPairs.get(i).get(j)._strColumnName.equals(pkey))) {
						flag = false;
						break;
					}
				}
				if (!flag) {
					break;
				}
			}
			if (flag) {
				// primary key select
				// new Sawy's code
				ArrayList<String> collection = new ArrayList<>();
				collection.add("AND");
				operatorsList.removeAll(collection);
				ArrayList<HashSet<Hashtable<String, Object>>> ANDedResult = getANDedPairsResultsSawy(ANDedPairs);
				ArrayList<ArrayList<HashSet<Hashtable<String, Object>>>> ORedPairs = new ArrayList<>();
				for (int i = 0; i < ANDedResult.size(); i++) {
					stack2.push(ANDedResult.get(i));
					if (i == ANDedResult.size() - 1 || operatorsList.size() == 0
							|| !operatorsList.get(i).equals("OR")) {
						ArrayList<HashSet<Hashtable<String, Object>>> list = new ArrayList<>();
						while (!stack2.isEmpty()) {
							list.add(stack2.pop());
						}
						ORedPairs.add(list);
					}
				}
				ArrayList<HashSet<Hashtable<String, Object>>> XORedPairs = new ArrayList<>();
				for (int i = 0; i < ORedPairs.size(); i++) {
					HashSet<Hashtable<String, Object>> pairResult = new HashSet<>();
					for (int j = 0; j < ORedPairs.get(i).size(); j++) {
						pairResult.addAll(ORedPairs.get(i).get(j));
					}
					XORedPairs.add(pairResult);
				}
				while (XORedPairs.size() > 1) {
					HashSet<Hashtable<String, Object>> Union = new HashSet<>();
					HashSet<Hashtable<String, Object>> Intersection = new HashSet<>();
					Union.addAll(XORedPairs.get(0));
					Union.addAll(XORedPairs.get(1));
					Intersection.addAll(XORedPairs.get(0));
					Intersection.retainAll(XORedPairs.get(1));
					Union.removeAll(Intersection);
					XORedPairs.add(0, Union);
					XORedPairs.remove(1);
					XORedPairs.remove(1);
				}
				HashSet<Hashtable<String, Object>> finalResultSet = XORedPairs.get(0);
				ArrayList<Hashtable<String, Object>> finalResultList = new ArrayList<>();
				finalResultList.addAll(finalResultSet);
				// sort by primary key if needed
				return finalResultList.iterator();
				// return the results...
			}
			// else go to
			// Linear Code
			else {
				String[] datatypes = new String[arrSQLTerms.length];

				try {
					BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
					String current = br.readLine();
					while (current != null) {
						if (current.length() != 0) {
							String arr[] = current.split(",");
							String tableName = arr[0];
							String columnName = arr[1];
							String datatype = arr[2];
							for (int i = 0; i < arrSQLTerms.length; i++) {
								if (tableName.equals(arrSQLTerms[i]._strTableName)
										&& columnName.equals(arrSQLTerms[i]._strColumnName) && arr[3].equals("true")) {

									datatypes[i] = datatype;
								} else if (tableName.equals(arrSQLTerms[i]._strTableName)
										&& columnName.equals(arrSQLTerms[i]._strColumnName)) {
									datatypes[i] = datatype;
								}
							}
						}
						current = br.readLine();
					}

					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				ArrayList<Hashtable<String, Object>> selectResultSet = new ArrayList<>();
				String tableName = arrSQLTerms[0]._strTableName;
				int noOfPages = countNumberOfPagesWithoutOverflows(tableName);
				int count = 0;
				for (int i = 1; i <= noOfPages; i++) {
					int noOfOverflows = countNumberOfPageOverflows(tableName, i);
					for (int j = 0; j <= noOfOverflows; j++) {
						Vector<Hashtable<String, Object>> page = readPageIntoVector(
								tableName + "[" + i + "](" + j + ").class");
						for (int k = 0; k < page.size(); k++) {
							boolean[] verifyarrSQLTerms = new boolean[arrSQLTerms.length];
							for (int x = 0; x < arrSQLTerms.length; x++) {
								String columnName = arrSQLTerms[x]._strColumnName;
								Object rowValue = page.get(k).get(columnName);
								String dataType = datatypes[x];

								if (arrSQLTerms[x]._strOperator.equals("=")) {
									verifyarrSQLTerms[x] = (compare(arrSQLTerms[x]._objValue, rowValue, dataType) == 0);
								} else if (arrSQLTerms[x]._strOperator.equals("!=")) {
									verifyarrSQLTerms[x] = (compare(arrSQLTerms[x]._objValue, rowValue, dataType) != 0);
								} else if (arrSQLTerms[x]._strOperator.equals("<")) {
									verifyarrSQLTerms[x] = (compare(rowValue, arrSQLTerms[x]._objValue, dataType) < 0);
								} else if (arrSQLTerms[x]._strOperator.equals(">")) {
									verifyarrSQLTerms[x] = (compare(rowValue, arrSQLTerms[x]._objValue, dataType) > 0);
								} else if (arrSQLTerms[x]._strOperator.equals("<=")) {
									verifyarrSQLTerms[x] = (compare(rowValue, arrSQLTerms[x]._objValue, dataType) <= 0);
								} else if (arrSQLTerms[x]._strOperator.equals(">=")) {
									verifyarrSQLTerms[x] = (compare(rowValue, arrSQLTerms[x]._objValue, dataType) >= 0);
								}
							}
							ArrayList<Boolean> resultsAfterAnding = new ArrayList<Boolean>();
							ArrayList<String> operatorsAfterAnding = new ArrayList<String>();
							resultsAfterAnding.add(verifyarrSQLTerms[0]);
							for (int y = 1; y < verifyarrSQLTerms.length; y++) {
								if (strarrOperators[y - 1].equals("AND")) {
									int lastElementIndex = resultsAfterAnding.size() - 1;
									resultsAfterAnding.set(lastElementIndex,
											resultsAfterAnding.get(lastElementIndex) & verifyarrSQLTerms[y]);
								} else {
									resultsAfterAnding.add(verifyarrSQLTerms[y]);
									operatorsAfterAnding.add(strarrOperators[y - 1]);
								}
							}

							ArrayList<Boolean> resultsAfterOring = new ArrayList<Boolean>();
							resultsAfterOring.add(resultsAfterAnding.get(0));
							for (int y = 1; y < resultsAfterAnding.size(); y++) {
								if (operatorsAfterAnding.get(y - 1).equals("OR")) {
									int lastElementIndex = resultsAfterOring.size() - 1;
									resultsAfterOring.set(lastElementIndex,
											resultsAfterOring.get(lastElementIndex) | resultsAfterAnding.get(y));
								} else {
									resultsAfterOring.add(resultsAfterAnding.get(y));
								}
							}

							boolean finalResultAfterXoring = resultsAfterOring.get(0);
							for (int y = 1; y < resultsAfterOring.size(); y++) {
								finalResultAfterXoring ^= resultsAfterOring.get(y);
							}
							if (finalResultAfterXoring) {
								selectResultSet.add(page.get(k));
							}
						}
					}
				}
				return selectResultSet.iterator();
			}
		}
	}

	public boolean checkAllPairsHaveGridsAndNoNotEqualOrHavePrimaryKey(ArrayList<ArrayList<SQLTerm>> Terms,
			ArrayList<Grid> Grids) {
		for (int i = 0; i < Terms.size(); i++) {
			HashSet<String> hashSetColumnName = new HashSet<>();
			boolean foundNotEqualPrimaryKey = false;

			for (int j = 0; j < Terms.get(i).size(); j++) {
				hashSetColumnName.add(Terms.get(i).get(j)._strColumnName);

				if (Terms.get(i).get(j)._strColumnName.equals(getPrimaryKeyName(Terms.get(i).get(j)._strTableName))
						&& Terms.get(i).get(j)._strOperator.equals("!=")) {
					return false;
				}

			}
			Grid g;
			if ((g = selectSuitableGridForSelect(Terms.get(i).get(0)._strTableName, hashSetColumnName)) == null
					& !checkPrimaryKeyExists(Terms.get(i).get(0)._strTableName, hashSetColumnName)) {

				return false;
			}
			if (g != null) {
				boolean flag = false;
				for (String name : g.namesAndDataTypes.keySet()) {
					for (int j = 0; j < Terms.get(i).size(); j++) {
						if (name.equals(Terms.get(i).get(j)._strColumnName)
								&& !Terms.get(i).get(j)._strOperator.equals("!=")) {
							flag = true;
						}
					}
				}
				if (!flag) {
					return false;
				}
			}
			Grids.add(g);
		}
		return true;
	}

	public boolean checkPrimaryKeyExists(String tableName, HashSet<String> hs) {

		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current;
			current = br.readLine();
			while (current != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(tableName)) {
					if (hs.contains(arr[1])) {
						if (arr[3].equals("true")) {
							return true;
						}
					}
				}
				current = br.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static String getPrimaryKeyName(String tableName) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current;
			current = br.readLine();
			while (current != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(tableName)) {
					if (arr[3].equals("true")) {
						return arr[1];
					}
				}
				current = br.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "lololol";
	}

	public static String getPrimaryKeyDataType(String tableName) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current;
			current = br.readLine();
			while (current != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(tableName)) {
					if (arr[3].equals("true")) {
						return arr[2];
					}
				}
				current = br.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "lololol";
	}

	public static String getColumnDataType(String tableName, String ColumnName) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current;
			current = br.readLine();
			while (current != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(tableName)) {
					if (arr[1].equals(ColumnName)) {
						return arr[2];
					}
				}
				current = br.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "lololol";
	}

	public HashSet<BucketItem> getBucketandOverflowsItems(String BucketName) {
		HashSet<BucketItem> hs = new HashSet<>();
		String bucketNameWithoutOverflow = "";
		int c = 0;
		while (BucketName.charAt(c) != '(') {
			bucketNameWithoutOverflow += BucketName.charAt(c);
			c++;
		}

		int x = Grid.getLastOverflowNumber(bucketNameWithoutOverflow);
		for (int i = 0; i <= x; i++) {
			hs.addAll(Grid.readBucketIntoVector(bucketNameWithoutOverflow + "(" + i + ").class"));
		}
		return hs;
	}

	public static String getBucketNameWithoutOverflow(String bucketName) {
		String bucketNameWithoutOverflow = "";
		int c = 0;
		while (bucketName.charAt(c) != '(') {
			bucketNameWithoutOverflow += bucketName.charAt(c);
			c++;
		}
		return bucketNameWithoutOverflow;
	}

	public HashSet<BucketItem> recursiveSelect(Grid g, Hashtable<String, ArrayList<SQLTerm>> ht, int level,
			Object array) {
		int minBlock = 0;
		Object minValue = null;
		boolean minEqual = false;
		int maxBlock = 9;
		Object maxValue = null;
		boolean maxEqual = false;
		ArrayList<Object> notEqualValues = new ArrayList<>();
		ArrayList<Object> EqualValues = new ArrayList<>();
		// get the name of the current level
		ArrayList<SQLTerm> levelConstraints = null;
		String currentLevelColumnName = null;
		for (Entry x : g.namesAndLevels.entrySet()) {
			if (((Integer) x.getValue()) == level) {
				currentLevelColumnName = (String) x.getKey();
			}
		}
		// if there are constraints on this level in the query
		// if there is not then just go through all the blocks in this level
		// if there are any constraints then we can never go into the null block
		if (ht.containsKey(currentLevelColumnName)) {
			levelConstraints = ht.get(currentLevelColumnName);
		} else {
			if (level == g.namesAndLevels.size() - 1) {
				// get the results from all the buckets and their overflows
				HashSet<BucketItem> levelResult = new HashSet<>();
				for (int i = 0; i < ((Object[]) array).length; i++) {
					String BucketName = (String) ((Object[]) array)[i];
					if (BucketName != null) {
						levelResult.addAll(getBucketandOverflowsItems(BucketName));
					}
				}
				return levelResult;
			} else {
				HashSet<BucketItem> levelResult = new HashSet<>();
				for (int i = 0; i <= 10; i++) {
					HashSet<BucketItem> blockResult = recursiveSelect(g, ht, level + 1, ((Object[]) array)[i]);
					levelResult.addAll(blockResult);
				}
				return levelResult;
			}
		}
		String thisLevelDataType = g.namesAndDataTypes.get(currentLevelColumnName);
		// loop over the level constraints and adjust the minEdge and maxEdge Blocks and
		// when values return ...
		// linear search and remove the less than min .. greater than max ... not equal
		// and equal stuff
		for (int i = 0; i < levelConstraints.size(); i++) {
			if (levelConstraints.get(i)._strOperator.equals(">") | levelConstraints.get(i)._strOperator.equals(">=")
					| levelConstraints.get(i)._strOperator.equals("<")
					| levelConstraints.get(i)._strOperator.equals("<=")) {
				
				if (levelConstraints.get(i)._strOperator.equals(">")
						| levelConstraints.get(i)._strOperator.equals(">=")) {
					
					if (compare(getMaximumOfColumn(levelConstraints.get(i)._strTableName, currentLevelColumnName),
							levelConstraints.get(i)._objValue, thisLevelDataType) <= 0) {
						
//						levelConstraints.get(i)._objValue = getMaximumOfColumn(levelConstraints.get(i)._strTableName,
//								currentLevelColumnName);
//						levelConstraints.get(i)._strOperator = ">";
						//if greater than or equal to the column maximum then no rows will be found so return empty result set
						return new HashSet<BucketItem>();
					}
				} else {
					
					if (compare(getMinimumOfColumn(levelConstraints.get(i)._strTableName, currentLevelColumnName),
							levelConstraints.get(i)._objValue, thisLevelDataType) > 0) {
//						levelConstraints.get(i)._objValue = getMinimumOfColumn(levelConstraints.get(i)._strTableName,
//								currentLevelColumnName);
//						levelConstraints.get(i)._strOperator = "<";
//						if less than the column minimum then no rows will be found so return empty result set
						return new HashSet<BucketItem>();
					}
				}

				// Now we need to get the position of this value in its level
				// We will binary search over the divisions
				int start = 0;
				int end = 9;
				int mid = 0;
				while (start <= end) {
					mid = (start + end) / 2;
					Object minOfDivision = g.ranges[level][mid].min;
					Object maxOfDivision = g.ranges[level][mid].max;
					int comparisonWithMin = DBApp.compare(levelConstraints.get(i)._objValue, minOfDivision,
							thisLevelDataType);
					int comparisonWithMax = DBApp.compare(levelConstraints.get(i)._objValue, maxOfDivision,
							thisLevelDataType);
					if (comparisonWithMin < 0) {
						end = mid - 1;
					} else {
						// if I reached the required block
						if (comparisonWithMax < 0) {
							if (levelConstraints.get(i)._strOperator.equals(">")
									| levelConstraints.get(i)._strOperator.equals(">=")) {
								// adjust the minBlock
								minBlock = Math.max(minBlock, mid);
								// compare last minValue to current minValue
								// if the was no previous minValue .. Just take the new Value and adjust the
								// minEqual
								if (minValue == null) {
									minValue = levelConstraints.get(i)._objValue;
									minEqual = levelConstraints.get(i)._strOperator.equals(">=") ? true : false;
								} else {
									// if it is greater than last minValue.. then it is the new minimum value
									// adjust the minEqual flag
									if (compare(levelConstraints.get(i)._objValue, minValue, thisLevelDataType) > 0) {
										minValue = levelConstraints.get(i)._objValue;
										minEqual = levelConstraints.get(i)._strOperator.equals(">=") ? true : false;
										// if it is equal to the last minValue.. then check if i don't have the equal
										// sign
										// adjust the minEqual flag
									} else if (compare(levelConstraints.get(i)._objValue, minValue,
											thisLevelDataType) == 0) {
										minEqual = levelConstraints.get(i)._strOperator.equals(">") ? false : minEqual;
									}
								}

							} else if (levelConstraints.get(i)._strOperator.equals("<")
									| levelConstraints.get(i)._strOperator.equals("<=")) {
								maxBlock = Math.min(maxBlock, mid);
								if (maxValue == null) {
									maxValue = levelConstraints.get(i)._objValue;
									maxEqual = levelConstraints.get(i)._strOperator.equals("<=") ? true : false;
								} else {
									// if it is smaller than last maxValue.. then it is the new maximum value
									// adjust the maxEqual flag
									if (compare(levelConstraints.get(i)._objValue, maxValue, thisLevelDataType) < 0) {
										maxValue = levelConstraints.get(i)._objValue;
										maxEqual = levelConstraints.get(i)._strOperator.equals("<=") ? true : false;
										// if it is equal to the last maxValue.. then check if i don't have the equal
										// sign
										// adjust the maxEqual flag
									} else if (compare(levelConstraints.get(i)._objValue, maxValue,
											thisLevelDataType) == 0) {
										maxEqual = levelConstraints.get(i)._strOperator.equals("<") ? false : maxEqual;
									}
								}
							}
							break;
						} else {
							start = mid + 1;
						}
					}
				}
			} else {
				if (levelConstraints.get(i)._strOperator.equals("=")) {
					EqualValues.add(levelConstraints.get(i)._objValue);
				} else if (levelConstraints.get(i)._strOperator.equals("!=")) {
					notEqualValues.add(levelConstraints.get(i)._objValue);
				}
			}
		}
		// for loop and go to the next levels
		// if edgemin block check for min constraints if edgemax check for max ...in all
		// block check for equal and not equal
		HashSet<BucketItem> levelResult = new HashSet<>();
		for (int i = minBlock; i <= maxBlock; i++) {
			HashSet<BucketItem> blockResult = new HashSet<>();
			// if i am at the last level then get the bucket items and filter according to
			// last level constraints
			if (level == g.namesAndLevels.size() - 1) {
				String BucketName = (String) ((Object[]) array)[i];
				if (BucketName != null) {
					blockResult = getBucketandOverflowsItems(BucketName);
				}
			} else {
				blockResult = recursiveSelect(g, ht, level + 1, ((Object[]) array)[i]);
			}
			HashSet<BucketItem> toBeRemoved = new HashSet<>();
			if (minValue != null) {
				if (i == minBlock) {
					for (BucketItem bi : blockResult) {
						if (minEqual) {
							if (compare(bi.colNamesAndValues.get(currentLevelColumnName), minValue,
									thisLevelDataType) < 0) {
								toBeRemoved.add(bi);
							}
						} else {
							if (compare(bi.colNamesAndValues.get(currentLevelColumnName), minValue,
									thisLevelDataType) < 0
									| compare(bi.colNamesAndValues.get(currentLevelColumnName), minValue,
											thisLevelDataType) == 0) {
								toBeRemoved.add(bi);
							}
						}
					}
				}
			}
			if (maxValue != null) {
				if (i == maxBlock) {
					for (BucketItem bi : blockResult) {
						if (maxEqual) {
							if (compare(bi.colNamesAndValues.get(currentLevelColumnName), maxValue,
									thisLevelDataType) > 0) {
								toBeRemoved.add(bi);
							}
						} else {
							if (compare(bi.colNamesAndValues.get(currentLevelColumnName), maxValue,
									thisLevelDataType) > 0
									| compare(bi.colNamesAndValues.get(currentLevelColumnName), maxValue,
											thisLevelDataType) == 0) {
								toBeRemoved.add(bi);
							}
						}
					}
				}
			}
			// check on equal and not equal
			for (BucketItem bi : blockResult) {
				for (int e = 0; e < EqualValues.size(); e++) {
					if (compare(bi.colNamesAndValues.get(currentLevelColumnName), EqualValues.get(e),
							thisLevelDataType) != 0) {
						toBeRemoved.add(bi);
					}
				}
				for (int e = 0; e < notEqualValues.size(); e++) {
					if (compare(bi.colNamesAndValues.get(currentLevelColumnName), notEqualValues.get(e),
							thisLevelDataType) == 0) {
						toBeRemoved.add(bi);
					}
				}
			}
			blockResult.removeAll(toBeRemoved);
			levelResult.addAll(blockResult);
		}
		return levelResult;
	}

	public Object getMinimumOfColumn(String tableName, String columnName) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current;
			current = br.readLine();
			while (current != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(tableName)) {
					if (arr[1].equals(columnName)) {
						if (arr[2].equals("java.lang.Integer")) {
							return Integer.parseInt(arr[5]);
						} else if (arr[2].equals("java.lang.Double")) {
							return Double.parseDouble(arr[5]);
						} else if (arr[2].equals("java.util.Date")) {
							String minDates[] = arr[5].split("-");
							return new Date(Integer.parseInt(minDates[0]) - 1900, Integer.parseInt(minDates[1]) - 1,
									Integer.parseInt(minDates[2]));
						} else {
							return arr[5];
						}
					}
				}
				current = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "lololol";
	}

	public Object getMaximumOfColumn(String tableName, String columnName) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current;
			current = br.readLine();
			while (current != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(tableName)) {
					if (arr[1].equals(columnName)) {
						if (arr[2].equals("java.lang.Integer")) {
							return Integer.parseInt(arr[6]);
						} else if (arr[2].equals("java.lang.Double")) {
							return Double.parseDouble(arr[6]);
						} else if (arr[2].equals("java.util.Date")) {
							String maxDates[] = arr[6].split("-");
							return new Date(Integer.parseInt(maxDates[0]) - 1900, Integer.parseInt(maxDates[1]) - 1,
									Integer.parseInt(maxDates[2]));
						} else {
							return arr[6];
						}
					}
				}
				current = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "lololol";
	}

	public ArrayList<HashSet<Hashtable<String, Object>>> getANDedPairsResult(ArrayList<ArrayList<SQLTerm>> Terms,
			ArrayList<Grid> Grids) {
		ArrayList<HashSet<BucketItem>> result = new ArrayList<>();
		ArrayList<HashSet<Hashtable<String, Object>>> sawyresult = new ArrayList<>();
		for (int i = 0; i < Terms.size(); i++) {
			// organizing each column with the constraints on it to decide what to do in
			// each level
			Hashtable<String, ArrayList<SQLTerm>> ht = new Hashtable<>();
			for (int j = 0; j < Terms.get(i).size(); j++) {
				if (ht.containsKey(Terms.get(i).get(j)._strColumnName)) {
					ht.get(Terms.get(i).get(j)._strColumnName).add(Terms.get(i).get(j));
				} else {
					ht.put(Terms.get(i).get(j)._strColumnName, new ArrayList<SQLTerm>());
					ht.get(Terms.get(i).get(j)._strColumnName).add(Terms.get(i).get(j));
				}
			}
			if (Grids.get(i) == null) {
				// result.add(Sawy's code)
				ArrayList<SQLTerm> list = new ArrayList<>();

				for (String column : ht.keySet()) {
					list.addAll(ht.get(column));
				}
				ArrayList<String> dataTypes = new ArrayList<>();
				for (SQLTerm term : list) {
					dataTypes.add(getColumnDataType(term._strTableName, term._strColumnName));
				}
				sawyresult.add(
						binarySearchPrimaryKeyAndNoIndex(list, getPrimaryKeyName(Terms.get(i).get(0)._strTableName),
								getPrimaryKeyDataType(Terms.get(i).get(0)._strTableName), dataTypes));
			} else {
				result.add(recursiveSelect(Grids.get(i), ht, 0, Grids.get(i).array));
			}
		}

		ArrayList<HashSet<Hashtable<String, Object>>> finalResult = loadBucketItemsintoRows(result,
				Terms.get(0).get(0)._strTableName);
		finalResult.addAll(sawyresult);
		return finalResult;
	}

	public ArrayList<HashSet<Hashtable<String, Object>>> getANDedPairsResultsSawy(ArrayList<ArrayList<SQLTerm>> Terms) {
		ArrayList<HashSet<Hashtable<String, Object>>> sawyresult = new ArrayList<>();
		for (int i = 0; i < Terms.size(); i++) {
			// organizing each column with the constraints on it to decide what to do in
			// each level
			Hashtable<String, ArrayList<SQLTerm>> ht = new Hashtable<>();
			for (int j = 0; j < Terms.get(i).size(); j++) {
				if (ht.containsKey(Terms.get(i).get(j)._strColumnName)) {
					ht.get(Terms.get(i).get(j)._strColumnName).add(Terms.get(i).get(j));
				} else {
					ht.put(Terms.get(i).get(j)._strColumnName, new ArrayList<SQLTerm>());
					ht.get(Terms.get(i).get(j)._strColumnName).add(Terms.get(i).get(j));
				}
			}
			// result.add(Sawy's code)
			ArrayList<SQLTerm> list = new ArrayList<>();
			for (String column : ht.keySet()) {
				list.addAll(ht.get(column));
			}
			ArrayList<String> dataTypes = new ArrayList<>();
			for (SQLTerm term : list) {
				dataTypes.add(getColumnDataType(term._strTableName, term._strColumnName));
			}
			sawyresult.add(binarySearchPrimaryKeyAndNoIndex(list, getPrimaryKeyName(Terms.get(i).get(0)._strTableName),
					getPrimaryKeyDataType(Terms.get(i).get(0)._strTableName), dataTypes));
		}
		return sawyresult;
	}

	public static ArrayList<HashSet<Hashtable<String, Object>>> loadBucketItemsintoRows(
			ArrayList<HashSet<BucketItem>> bi, String tableName) {
		ArrayList<HashSet<Hashtable<String, Object>>> finalResult = new ArrayList<>();
		for (int i = 0; i < bi.size(); i++) {
			Hashtable<String, ArrayList<Object>> pageNamePrimaryKeys = new Hashtable<>();
			for (BucketItem item : bi.get(i)) {
				if (pageNamePrimaryKeys.containsKey(item.pageName)) {
					pageNamePrimaryKeys.get(item.pageName).add(item.primaryKeyValue);
				} else {
					ArrayList<Object> list = new ArrayList<>();
					list.add(item.primaryKeyValue);
					pageNamePrimaryKeys.put(item.pageName, list);
				}
			}
			finalResult.add(loadPageAndGetRows(pageNamePrimaryKeys, tableName));
		}
		return finalResult;
	}

	public static HashSet<Hashtable<String, Object>> loadPageAndGetRows(
			Hashtable<String, ArrayList<Object>> pageNamePrimaryKeys, String tableName) {
		HashSet<Hashtable<String, Object>> result = new HashSet<>();
		String primaryKeyColumnName = getPrimaryKeyName(tableName);
		for (String pageName : pageNamePrimaryKeys.keySet()) {
			Vector<Hashtable<String, Object>> page = readPageIntoVector(pageName);
			for (Hashtable<String, Object> row : page) {
				if (pageNamePrimaryKeys.get(pageName).contains(row.get(primaryKeyColumnName))) {
					result.add(row);
				}
			}
		}
		return result;
	}

	public static boolean checkTableExists(String tableName) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			while (current != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(tableName)) {
					return true;
				}
				current = br.readLine();
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void checkCreateTableExceptions(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		// check if table already exists
		if (checkTableExists(strTableName)) {
			throw new DBAppException("Table already exists!");
		}
		// check inserted dataTypes
		checkDataTypeAndPrimaryExists(htblColNameType, strClusteringKeyColumn, htblColNameMin, htblColNameMax);
	}

	public void checkDataTypeAndPrimaryExists(Hashtable<String, String> htblColNameType, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {

		if (htblColNameType.size() != htblColNameMin.size() && htblColNameType.size() != htblColNameMax.size()) {
			throw new DBAppException("The inserted hashtables are not of same size");
		}

		boolean clusterFound = false;
		Set<String> keys = htblColNameType.keySet();
		for (String key : keys) {
			// Check if primary key exists
			if (!htblColNameMax.containsKey(key)) {
				throw new DBAppException("Column " + key + " has no maximum value inserted");
			} else if (!htblColNameMin.containsKey(key)) {
				throw new DBAppException("Column " + key + " has no minimum value inserted");
			}
			if (key.equals(strClusteringKeyColumn)) {
				clusterFound = true;
			}
			String dataType = htblColNameType.get(key);
			if (!(dataType.equals("java.lang.Integer") || dataType.equals("java.lang.String")
					|| dataType.equals("java.lang.Double") || dataType.equals("java.util.Date"))) {
				throw new DBAppException("Column " + key + " has an unsupported data type");

			} else if (dataType.equals("java.lang.Integer")) {
				try {
					Integer.parseInt(htblColNameMax.get(key));
				} catch (Exception e) {
					throw new DBAppException("Column " + key + " maximum value is not an integer");
				}
				try {
					Integer.parseInt(htblColNameMin.get(key));
				} catch (Exception e) {
					throw new DBAppException("Column " + key + " minimum value is not an integer");
				}

			} else if (dataType.equals("java.lang.Double")) {
				try {
					Double.parseDouble(htblColNameMax.get(key));
				} catch (Exception e) {
					throw new DBAppException("Column " + key + " maximum value is not a Double");
				}
				try {
					Double.parseDouble(htblColNameMin.get(key));
				} catch (Exception e) {
					throw new DBAppException("Column " + key + " minimum value is not a Double");
				}

			} else if (dataType.equals("java.util.Date")) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				try {
					format.parse(htblColNameMax.get(key));
				} catch (ParseException e) {
					throw new DBAppException(
							"Column " + key + " maximum value has wrong date format, Make sure it's (YYYY-MM-DD)");
				}
				try {
					format.parse(htblColNameMin.get(key));
				} catch (ParseException e) {
					throw new DBAppException(
							"Column " + key + " minimum value has wrong date format, Make sure it's (YYYY-MM-DD)");
				}
			}
		}
		if (!clusterFound) {
			throw new DBAppException("Primary key not found!");
		}
	}

	public void checkUpdateConstraints(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (!checkTableExists(strTableName)) {
			throw new DBAppException("Table does not exist!");
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			int countCorrectColumns = 0;
			while (current != null) {
				String arr[] = current.split(",");
				if (arr[0].equals(strTableName)) {
					if (htblColNameValue.containsKey(arr[1])) {
						if (arr[2].equals("java.lang.Integer")) {
							int compareToMin = compare(htblColNameValue.get(arr[1]), Integer.parseInt(arr[5]), arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]), Integer.parseInt(arr[6]), arr[2]);

							if (compareToMin < 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is below the minimum value");
							}
							if (compareToMax > 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is above the maximum");
							}
						} else if (arr[2].equals("java.lang.Double")) {
							int compareToMin = compare(htblColNameValue.get(arr[1]), Double.parseDouble(arr[5]),
									arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]), Double.parseDouble(arr[6]),
									arr[2]);

							if (compareToMin < 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is below the minimum value");
							}
							if (compareToMax > 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is above the maximum value");
							}
						} else if (arr[2].equals("java.util.Date")) {
							String minDates[] = arr[5].split("-");
							String maxDates[] = arr[6].split("-");
							int compareToMin = compare(htblColNameValue.get(arr[1]),
									new Date(Integer.parseInt(minDates[0]) - 1900, Integer.parseInt(minDates[1]) - 1,
											Integer.parseInt(minDates[2])),
									arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]),
									new Date(Integer.parseInt(maxDates[0]) - 1900, Integer.parseInt(maxDates[1]) - 1,
											Integer.parseInt(maxDates[2])),
									arr[2]);

							if (compareToMin < 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is below the minimum value");
							}
							if (compareToMax > 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is above the maximum value");
							}
						} else {
							int compareToMin = compare(htblColNameValue.get(arr[1]), arr[5], arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]), arr[6], arr[2]);

							if (compareToMin < 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is below the minimum value");
							}
							if (compareToMax > 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is above the maximum value");
							}
						}

						countCorrectColumns++;
					}
				}
				current = br.readLine();
			}

			br.close();
			if (!(countCorrectColumns == htblColNameValue.size())) {
				throw new DBAppException("Hashtable Columns are not in metadata!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void checkInsertConstraints(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		// 5 min 6 max
		if (!checkTableExists(strTableName)) {
			throw new DBAppException("Table does not exist!");
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			// check if column names in hashtable exist in metadata
			int countCorrectColumns = 0;
			boolean primaryKeyFound = false;
			while (current != null) {
				String arr[] = current.split(",");
				// check if metadata row has same table name as input
				if (arr[0].equals(strTableName)) {
					// check if hashtable contains same column name as metadata
					if (htblColNameValue.containsKey(arr[1])) {
						// check if this is primary key
						if (arr[3].equals("true")) {
							primaryKeyFound = true;
						}
						// check if metadata row has same datatype of input value
						try {
							if (!Class.forName(arr[2]).isInstance(htblColNameValue.get(arr[1]))) {
								throw new DBAppException("Wrong datatype in column " + arr[1] + " !");
							}
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}

						if (arr[2].equals("java.lang.Integer")) {

							int compareToMin = compare(htblColNameValue.get(arr[1]), Integer.parseInt(arr[5]), arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]), Integer.parseInt(arr[6]), arr[2]);

							if (compareToMin < 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is below the minimum value");
							}
							if (compareToMax > 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is above the maximum");
							}
						} else if (arr[2].equals("java.lang.Double")) {
							int compareToMin = compare(htblColNameValue.get(arr[1]), Double.parseDouble(arr[5]),
									arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]), Double.parseDouble(arr[6]),
									arr[2]);

							if (compareToMin < 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is below the minimum value");
							}
							if (compareToMax > 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is above the maximum value");
							}
						} else if (arr[2].equals("java.util.Date")) {
							String minDates[] = arr[5].split("-");
							String maxDates[] = arr[6].split("-");
							int compareToMin = compare(htblColNameValue.get(arr[1]),
									new Date(Integer.parseInt(minDates[0]) - 1900, Integer.parseInt(minDates[1]) - 1,
											Integer.parseInt(minDates[2])),
									arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]),
									new Date(Integer.parseInt(maxDates[0]) - 1900, Integer.parseInt(maxDates[1]) - 1,
											Integer.parseInt(maxDates[2])),
									arr[2]);

							if (compareToMin < 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is below the minimum value");
							}
							if (compareToMax > 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is above the maximum value");
							}
						} else {
							int compareToMin = compare(htblColNameValue.get(arr[1]), arr[5], arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]), arr[6], arr[2]);

							if (compareToMin < 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is below the minimum value");
							}
							if (compareToMax > 0) {
								throw new DBAppException(
										"The value inserted in column " + arr[1] + " is above the maximum value");
							}
						}
						countCorrectColumns++;
					}
				}
				current = br.readLine();
			}
			br.close();
			if (!(countCorrectColumns == htblColNameValue.size())) {
				throw new DBAppException("Hashtable Columns are not in metadata!");
			}
			if (!primaryKeyFound) {
				throw new DBAppException("Primary key not inserted!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String getFileTableName(String fileName) {
		String tableName = "";
		int c = 0;
		while (fileName.charAt(c) != '[') {
			tableName += fileName.charAt(c);
			c++;
		}
		return tableName;
	}

	public static int getFilePageNumber(String fileName) {
		String pageNumber = "";
		int c = 0;
		while (fileName.charAt(c) != '[') {
			c++;
		}
		c++;
		while (fileName.charAt(c) != ']') {
			pageNumber += fileName.charAt(c);
			c++;
		}
		return Integer.parseInt(pageNumber);
	}

	public static int getFileOverflowNumber(String fileName) {
		String overflowNumber = "";
		int c = 0;
		while (fileName.charAt(c) != '(') {
			c++;
		}
		c++;
		while (fileName.charAt(c) != ')') {
			overflowNumber += fileName.charAt(c);
			c++;
		}
		return Integer.parseInt(overflowNumber);
	}

	public String binarySearchOnPages(String tableName, int totalNumberOfPages, String primaryKey,
			Object primaryKeyValue, String primaryKeyType) throws DBAppException {
		int start = 1;
		int end = totalNumberOfPages;

		// checking if the value of the input is less than
		// the minimum value in the table and returning the first page
		Vector<Hashtable<String, Object>> firstPage = readPageIntoVector(tableName + "[1](0).class");
		Object firstPageMin = firstPage.get(0).get(primaryKey);
		int compareWithFirstPageMin = compare(primaryKeyValue, firstPageMin, primaryKeyType);
		if (compareWithFirstPageMin < 0) {
			return tableName + "[1](0).class";
		}

		// Binary Search to get the page
		while (start <= end) {
			int mid = (start + end) / 2;
			Vector<Hashtable<String, Object>> currentPage = readPageIntoVector(tableName + "[" + mid + "](0).class");
			Object maxValue = null;

			// Getting the minimum value of the page
			Object minValue = currentPage.get(0).get(primaryKey);

			// Getting the number of overflow pages of this page
			int numberOfOverFlows = countNumberOfPageOverflows(tableName, mid);

			// If the numberOfOverFlows is zero then the maximum value is the maximum of
			// current page
			if (numberOfOverFlows == 0) {
				maxValue = currentPage.get(currentPage.size() - 1).get(primaryKey);
			} else {
				// if it's not 0 then we get the maximum value from the last overflow page
				Vector<Hashtable<String, Object>> lastOverflowPage = readPageIntoVector(
						tableName + "[" + mid + "](" + numberOfOverFlows + ").class");
				maxValue = lastOverflowPage.get(lastOverflowPage.size() - 1).get(primaryKey);
			}

			// we compare the primary value to the minimum value
			int compareToMinValue = compare(primaryKeyValue, minValue, primaryKeyType);
			if (compareToMinValue == 0) {
				throw new DBAppException("The primary key already exists!");
			} else if (compareToMinValue < 0) {
				// if it is less than the minimum value then we take the part before the
				// current/middle page
				end = mid - 1;
			} else if (compareToMinValue > 0) {
				// else if it is greater than the minimum
				// compare the primary value to the max value
				int compareToMaxValue = compare(primaryKeyValue, maxValue, primaryKeyType);
				if (compareToMaxValue == 0) {
					throw new DBAppException("The primary key already exists!");
				} else if (compareToMaxValue > 0) {
					// If it is grater than the maximum value then we have two cases

					// First Case:
					// if it is less than the minimum of the next page then we know it has to
					// be inserted in the last overflow of the current page(if exists)
					int next = mid + 1;
					// if we are not already on the last page
					if (next <= totalNumberOfPages) {
						Vector<Hashtable<String, Object>> nextPage = readPageIntoVector(
								tableName + "[" + next + "](0).class");
						Object minofnextpage = nextPage.get(0).get(primaryKey);
						int compareToMinOfNextPage = compare(primaryKeyValue, minofnextpage, primaryKeyType);
						if (compareToMinOfNextPage < 0) {
							// it must be inserted at the end of the last overflow of this page
							return tableName + "[" + mid + "](" + numberOfOverFlows + ").class";
						} else if (compareToMinOfNextPage == 0) {
							// if it is equal to the minimum of the next page then we found it
							throw new DBAppException("The primary key already exists!");
						} else {
							// or if its is not then we take the first half of the pages
							start = mid + 1;
						}
					} else {
						// if we are already on the last page(no next page) and the key is greater than
						// the maximum
						// return last overflow of the current page which is the last page
						return tableName + "[" + mid + "](" + numberOfOverFlows + ").class";
					}
				} else if (compareToMaxValue < 0) {
					// if the primary value is less than the maximum then its in this page or one of
					// its overflows
					// if there is no overflows we return this page
					if (numberOfOverFlows == 0) {
						return tableName + "[" + mid + "](0).class";
					} else {
						// else we binary search on the page and its overflows to find the correct page
						String overFlowResult = binarySearchOnOverflowPages(tableName, numberOfOverFlows, primaryKey,
								primaryKeyValue, primaryKeyType, mid);
						return overFlowResult;
					}
				}
			}
		}

		return "";
	}

	// searching for the overflow page needed to store the new input
	public String binarySearchOnOverflowPages(String tableName, int totalNumberOfOverflowPages, String primaryKey,
			Object primaryKeyValue, String primaryKeyType, int page) throws DBAppException {
		int start = 0;
		int end = totalNumberOfOverflowPages;
		while (start <= end) {
			int mid = (start + end) / 2;
			// here after getting the middle page of the overflow pages of this specific
			// page we get its path
			// getting this over flow page to check if its in the range of this overflow
			// page
			Vector<Hashtable<String, Object>> currentPage = readPageIntoVector(
					tableName + "[" + page + "](" + mid + ").class");

			// max value of this overflow page
			Object maxValue = currentPage.get(currentPage.size() - 1).get(primaryKey);
			// min value of this overflow page
			Object minValue = currentPage.get(0).get(primaryKey);

			// comparing the primary key value to the minimum value
			int compareToMinValue = compare(primaryKeyValue, minValue, primaryKeyType);
			if (compareToMinValue == 0) {
				throw new DBAppException("Primary key already exists!");
			}
			// if the primary value < minimum value we take the half before the mid
			else if (compareToMinValue < 0) {
				end = mid - 1;
			} else {
				// else we compare it to the max value
				int compareToMaxValue = compare(primaryKeyValue, maxValue, primaryKeyType);
				if (compareToMaxValue == 0) {
					throw new DBAppException("Primary key already exists!");
				}
				// if it is less than the maximum value then its in in this over flow page
				else if (compareToMaxValue < 0) {
					return tableName + "[" + page + "](" + mid + ").class";
				}
				// if it is greater than the max val then we have two cases
				else {
					// if it is less than the min of the next page
					// then it has to be inserted in this current page
					int next = mid + 1;
					if (next <= totalNumberOfOverflowPages) { // if we are not in the last page
						Vector<Hashtable<String, Object>> nextPage = readPageIntoVector(
								tableName + "[" + page + "](" + next + ").class");

						Object minOfNextPage = nextPage.get(0).get(primaryKey);
						int compareToMinOfNextPage = compare(primaryKeyValue, minOfNextPage, primaryKeyType);
						if (compareToMinOfNextPage < 0) {
							// insert in the this page
							return tableName + "[" + page + "](" + mid + ").class";
						} else if (compareToMinOfNextPage == 0) {
							throw new DBAppException("The primary key already exists!");
						} else {
							// if it is not less than the minimum of the next page then we continue binary
							// search
							start = mid + 1;
						}
					} else {
						// if we are already in the last page and the key is greater than its maximum
						// then it should
						// be inserted in this last page
						return tableName + "[" + page + "](" + mid + ").class";
					}
				}
			}
		}
		return "";
	}

	public static int changeStringToInt(String input) {
		int base = 10000000;
		int res = 0;
		int count = 0;
		while (base >= 1 && count < input.length()) {
			res = res + input.charAt(count) * base;
			base = base / 100;
			count++;
		}
		return res;
	}

	public static int comapreForRanges1(Object obj1, Object obj2) {
		int num1 = changeStringToInt((String) obj1);
		int num2 = (int)obj2;
		if (num1 > num2)
			return 1;
		else if (num1 < num2)
			return -1;
		else
			return 0;
	}
	public static int comapreForRanges2(Object obj1, Object obj2) {
		int num2 = changeStringToInt((String) obj2);
		int num1 = (int)obj1;
		if (num1 > num2)
			return 1;
		else if (num1 < num2)
			return -1;
		else
			return 0;
	}
	public static int comapreForRanges3(Object obj1, Object obj2) {
		int num1 = (int)obj1;
		int num2 = (int)obj2;
		if (num1 > num2)
			return 1;
		else if (num1 < num2)
			return -1;
		else
			return 0;
	}

	// comparing objects
	public static int compare(Object obj1, Object obj2, String primaryKeyType) {

		if (primaryKeyType.equals("java.lang.Double")) {
			if (((double) obj1) > ((double) obj2))
				return 1;
			else if (((double) obj1) < ((double) obj2))
				return -1;
			else
				return 0;
		} else if (primaryKeyType.equals("java.lang.Integer")) {
			if (((int) obj1) > ((int) obj2))
				return 1;
			else if (((int) obj1) < ((int) obj2))
				return -1;
			else
				return 0;
		} else if (primaryKeyType.equals("java.util.Date")) {
			return ((Date) obj1).compareTo((Date) obj2);
		} else if (primaryKeyType.equals("java.lang.String")) {
			int x =-1;
			try {
				x= (int)obj2;
			}catch(Exception e){
				
			}
			int y =-1;
			try {
				y= (int)obj1;
			}catch(Exception e){
				
			}
			if (x !=-1 && y!=-1) {
				return comapreForRanges3(obj1, obj2);
			}
			else if (x!=-1) {
				return comapreForRanges1(obj1, obj2);
			} else if(y!=-1){
				return comapreForRanges2(obj1, obj2);
			}else {
				return ((String) obj1).compareTo((String) obj2);
			}
		}
		return -100;
	}

	// count the pages for a specific table
	public static int countNumberOfPagesWithoutOverflows(String Tablename) {
		File dir = new File("src\\main\\resources\\data");
		File[] directoryListing = dir.listFiles();
		int counter = 0;
		if (directoryListing != null) {
			for (File page : directoryListing) {
				String tableName = getFileTableName(page.getName());
				int overflowno = getFileOverflowNumber(page.getName());
				if (tableName.equals(Tablename) && overflowno == 0) {
					counter++;
				}
			}
		}
		return counter;
	}

	// count the overflow pages for a specific page
	public static int countNumberOfPageOverflows(String Tablename, int Number) {

		File dir = new File("src\\main\\resources\\data");
		File[] directoryListing = dir.listFiles();
		int counter = 0;
		if (directoryListing != null) {
			for (File page : directoryListing) {
				String tableName = getFileTableName(page.getName());
				int overflowNumber = getFileOverflowNumber(page.getName());
				int pageNumber = getFilePageNumber(page.getName());

				if (tableName.equals(Tablename) && pageNumber == Number && overflowNumber != 0) {
					counter++;
				}
			}
		}
		return counter;
	}

	public static Vector<Hashtable<String, Object>> readPageIntoVector(String pageName) {
		String path = "src\\main\\resources\\data\\" + pageName;
		Vector<Hashtable<String, Object>> v = null;
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

	public static Vector<BucketItem> readBucketIntoVector(String bucketName) {
		String path = "src\\main\\resources\\indicesAndBuckets\\" + bucketName;
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

	public void linearDeleteOnTable(String strTableName, Hashtable<String, Object> htblColNameValue) {
		String Primarykey = "";
		String PrimarykeyType = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			// check if column names in hashtable exist in metadata
			int countCorrectColumns = 0;
			while (current != null) {
				String arr[] = current.split(",");
				// check if metadata row has same table name as input
				if (arr[0].equals(strTableName)) {
					// check if hashtable contains same column name as metadata
					if (htblColNameValue.containsKey(arr[1])) {
						//
						// check if this column is the primary key
						if (arr[3].equals("true")) {
							Primarykey = arr[1];
							PrimarykeyType = arr[2];
						}
					}
				}
				current = br.readLine();
			}
		} catch (Exception e) {
		}
		int TotalNumberOfPages = countNumberOfPagesWithoutOverflows(strTableName);
		// Loop on all pages
		for (int i = TotalNumberOfPages; i >= 1; i--) {
			// First we loop on the overflows from the last overflow page to the first
			int TotalNumberOfOverflowPages = countNumberOfPageOverflows(strTableName, i);
			for (int j = TotalNumberOfOverflowPages; j >= 0; j--) {
				// Loop on current page
				Vector<Hashtable<String, Object>> Page = readPageIntoVector(
						strTableName + "[" + i + "](" + j + ").class");
				for (int row = 0; row < Page.size(); row++) {
					// Loop on hashtable columns to check if all its values are equal to row values
					// or not
					boolean checkDelete = true;
					Set<String> columns = htblColNameValue.keySet();
					for (String column : columns) {
						if (!Page.get(row).get(column).equals(htblColNameValue.get(column))) {
							checkDelete = false; // If false then we do not want to delete this row
						}
					}
					if (checkDelete) {
						Hashtable<String, Object> removed = Page.remove(row);
						BucketItem s = deleteRowFromIndexes(strTableName, removed, Primarykey, PrimarykeyType);
						try {
							FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\"
									+ strTableName + "[" + i + "](" + j + ")" + ".class");
							ObjectOutputStream out = new ObjectOutputStream(fileOut);
							out.writeObject(Page);
							out.close();
							fileOut.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				// Check if page is empty
				if (j == 0) { // Check if it is not an overflow page
					if (Page.size() == 0) { // If the page is empty (size of vector==0), we delete it
						String removedPage = strTableName + "[" + i + "](" + j + ").class";
						shiftBucketPageNames(removedPage);
						File filetobedeleted = new File(
								"src\\main\\resources\\data\\" + strTableName + "[" + i + "](" + j + ").class");
						filetobedeleted.delete();

						// Check if the deleted page had any overflows
						if (TotalNumberOfOverflowPages == 0) {
							// If no, we shift all the pages(and their overflows) under it up one step
							for (int x = i + 1; x <= TotalNumberOfPages; x++) {
								int temp = x - 1;
								int TotalNumberOfOverflowPages2 = countNumberOfPageOverflows(strTableName, x);
								for (int k = 0; k <= TotalNumberOfOverflowPages2; k++) {
									File oldfile = new File("src\\main\\resources\\data\\" + strTableName + "[" + x
											+ "](" + k + ").class");
									File newfile = new File("src\\main\\resources\\data\\" + strTableName + "[" + temp
											+ "](" + k + ").class");
									oldfile.renameTo(newfile);
								}
							}
						} else {
							// If Yes(it did have overflows), we shift only the overflows to the left one
							// step
							for (int l = 1; l <= TotalNumberOfOverflowPages; l++) {

								File oldfile = new File(
										"src\\main\\resources\\data\\" + strTableName + "[" + i + "](" + l + ").class");
								int temp = l - 1;
								File newfile = new File("src\\main\\resources\\data\\" + strTableName + "[" + i + "]("
										+ temp + ").class");
								oldfile.renameTo(newfile);
							}
						}
					}
				} else { // it is an overflow page
					if (Page.size() == 0) { // If the page is empty (size of vector==0), we delete it
						String removedPage = strTableName + "[" + i + "](" + j + ").class";
						shiftBucketPageNames(removedPage);
						File f1 = new File(
								"src\\main\\resources\\data\\" + strTableName + "[" + i + "](" + j + ").class"); // file
																													// to
						f1.delete();
						// Decrement all overflow pages with overflow number > j
						for (int x = j + 1; x <= TotalNumberOfOverflowPages; x++) {
							File f2 = new File(
									"src\\main\\resources\\data\\" + strTableName + "[" + i + "](" + x + ").class");
							int temp = x - 1;
							File f3 = new File(
									"src\\main\\resources\\data\\" + strTableName + "[" + i + "](" + temp + ").class");
							f2.renameTo(f3);
						}
					}
				}
			}
		}
	}

	public String binaryDeleteFromTableOverflow(String tableName, int totalNumberOfOverflowPages, String primaryKey,
			Object primaryKeyValue, String primaryKeyType, int page) {
		int start = 0;
		int end = totalNumberOfOverflowPages;
		while (start <= end) {
			int mid = (start + end) / 2;
			// here after getting the middle page of the overflow pages of this specific
			// page we get its path
			// getting this over flow page to check if its in the range of this overflow
			// page
			Vector<Hashtable<String, Object>> currentPage = readPageIntoVector(
					tableName + "[" + page + "](" + mid + ").class");

			// max value of this overflow page
			Object maxValue = currentPage.get(currentPage.size() - 1).get(primaryKey);
			// min value of this overflow page
			Object minValue = currentPage.get(0).get(primaryKey);

			// comparing the primary key value to the minimum value
			int compareToMinValue = compare(primaryKeyValue, minValue, primaryKeyType);
			if (compareToMinValue == 0) {
				return tableName + "[" + page + "](" + mid + ").class";
			}
			// if the primary value < minimum value we take the half before the mid
			else if (compareToMinValue < 0) {
				end = mid - 1;
			} else {
				// else we compare it to the max value
				int compareToMaxValue = compare(primaryKeyValue, maxValue, primaryKeyType);
				if (compareToMaxValue == 0) {
					return tableName + "[" + page + "](" + mid + ").class";
				}
				// if it is less than the maximum value then its in in this over flow page
				else if (compareToMaxValue < 0) {
					return tableName + "[" + page + "](" + mid + ").class";
				} else {
					start = mid + 1;
				}
			}
		}
		return "";
	}

	public String binaryDeleteFromTable(String tableName, Hashtable<String, Object> htblColNameValue,
			String primaryKeyColumn, String primaryKeyType) {
		int totalNumberOfPages = countNumberOfPagesWithoutOverflows(tableName);
		Object primaryKeyValue = htblColNameValue.get(primaryKeyColumn);
		int start = 1;
		int end = totalNumberOfPages;
		// Binary Search to get the page
		while (start <= end) {
			int mid = (start + end) / 2;
			Vector<Hashtable<String, Object>> currentPage = readPageIntoVector(tableName + "[" + mid + "](0).class");
			Object maxValue = null;

			// Getting the minimum value of the page
			Object minValue = currentPage.get(0).get(primaryKeyColumn);

			// Getting the number of overflow pages of this page
			int numberOfOverFlows = countNumberOfPageOverflows(tableName, mid);

			// If the numberOfOverFlows is zero then the maximum value is the maximum of
			// current page
			if (numberOfOverFlows == 0) {
				maxValue = currentPage.get(currentPage.size() - 1).get(primaryKeyColumn);
			} else {
				// if it's not 0 then we get the maximum value from the last overflow page
				Vector<Hashtable<String, Object>> lastOverflowPage = readPageIntoVector(
						tableName + "[" + mid + "](" + numberOfOverFlows + ").class");
				maxValue = lastOverflowPage.get(lastOverflowPage.size() - 1).get(primaryKeyColumn);
			}
			// we compare the primary value to the minimum value
			int compareToMinValue = compare(primaryKeyValue, minValue, primaryKeyType);
			if (compareToMinValue == 0) {
				return tableName + "[" + mid + "]" + "(0).class";
			} else if (compareToMinValue < 0) {
				// if it is less than the minimum value then we take the part before the
				// current/middle page
				end = mid - 1;
			} else if (compareToMinValue > 0) {
				// else if it is greater than the minimum
				// compare the primary value to the max value
				int compareToMaxValue = compare(primaryKeyValue, maxValue, primaryKeyType);
				if (compareToMaxValue == 0) {
					return tableName + "[" + mid + "](" + numberOfOverFlows + ").class";
				} else if (compareToMaxValue > 0) {
					// If it is greater than the maximum then we proceed with the binary search
					start = mid + 1;
				} else if (compareToMaxValue < 0) {
					// if the primary value is less than the maximum then its in this page or one of
					// its overflows
					// if there is no overflows we return this page
					if (numberOfOverFlows == 0) {
						return tableName + "[" + mid + "](0).class";
					} else {
						// else we binary search on the page and its overflows to find the correct page
						String overFlowResult = binaryDeleteFromTableOverflow(tableName, numberOfOverFlows,
								primaryKeyColumn, primaryKeyValue, primaryKeyType, mid);
						return overFlowResult;
					}
				}
			}
		}
		return "";
	}

	public String checkDeleteConstraints(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		String Primarykey = "";
		String PrimarykeyType = "";
		if (!checkTableExists(strTableName)) {
			throw new DBAppException("Table does not exist!");
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			// check if column names in hashtable exist in metadata
			int countCorrectColumns = 0;
			while (current != null) {
				String arr[] = current.split(",");
				// check if metadata row has same table name as input
				if (arr[0].equals(strTableName)) {
					// check if hashtable contains same column name as metadata
					if (htblColNameValue.containsKey(arr[1])) {
						//
						// check if this column is the primary key
						if (arr[3].equals("true")) {
							Primarykey = arr[1];
							PrimarykeyType = arr[2];
						}
						// check if metadata row has same datatype of input value
						try {
							if (!Class.forName(arr[2]).isInstance(htblColNameValue.get(arr[1]))) {
								throw new DBAppException("Wrong datatype in column" + arr[1] + " !");
							}
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						countCorrectColumns++;
					}
				}
				current = br.readLine();
			}

			br.close();
			if (!(countCorrectColumns == htblColNameValue.size())) {
				throw new DBAppException("Hashtable Columns are not in metadata!");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return Primarykey + " " + PrimarykeyType;
	}

	public String getPageAndIndex(String strTableName, Object primaryKeyValue, String primaryKeyColumn,
			String primarykeyType) throws DBAppException {

		Hashtable<String, Object> h1 = new Hashtable<>();
		h1.put(primaryKeyColumn, primaryKeyValue);
		String pageToDeleteFrom = binaryDeleteFromTable(strTableName, h1, primaryKeyColumn, primarykeyType);

		if (!(pageToDeleteFrom.equals(""))) {
			Vector<Hashtable<String, Object>> pageVector = readPageIntoVector(pageToDeleteFrom);
			int start = 0;
			int end = pageVector.size() - 1;

			// Binary Search to get the page
			while (start <= end) {
				int mid = (start + end) / 2;

				int comparison = compare(primaryKeyValue, pageVector.get(mid).get(primaryKeyColumn), primarykeyType);
				if (comparison == 0) {
					return pageToDeleteFrom + " " + mid;
				} else if (comparison > 0) {
					start = mid + 1;
				} else {
					end = mid - 1;
				}
			}
			throw new DBAppException("No row where this primary key exists!");
		}
		return "";

	}

	public void changePageNameInAllIndices(String tableName, Hashtable<String, Object> row, String newPageName) {

		ArrayList<Grid> allIndicesOfCurrentTable = (ArrayList<Grid>) this.allIndexes.get(tableName);

		if (allIndicesOfCurrentTable == null) {
			return;
		}
		// loop on all indices
		for (int i = 0; i < allIndicesOfCurrentTable.size(); i++) {
			Grid currentIndex = allIndicesOfCurrentTable.get(i);
			currentIndex.changePageNameInIndex(newPageName, row);
		}
	}

	public String binarySearchAndInsertInPages(String primaryKey, String tableName, int pageNumber, int overflowNumber,
			Object primaryKeyValue, String primaryKeyType, Hashtable<String, Object> record) throws DBAppException {
		// set current page path according to parameters and then read into a vector
		String currentPagePath = tableName + "[" + pageNumber + "](" + overflowNumber + ").class";
		Vector<Hashtable<String, Object>> currentPage = readPageIntoVector(currentPagePath);
		String pageInsertedInto = currentPagePath;
		// binary search to find insertion position
		int startValue = 0;
		int endValue = currentPage.size() - 1;
		int mid = 0;
		while (startValue <= endValue) {
			mid = (startValue + endValue) / 2;
			int compare = compare(primaryKeyValue, currentPage.get(mid).get(primaryKey), primaryKeyType);
			if (compare == 0)
				throw new DBAppException("Primary key already exists!");
			else if (compare < 0) {
				endValue = mid - 1;
			} else {
				startValue = mid + 1;
			}
		}
		// inserting according to binary search
		int insertionPosition = mid;
		int compare = compare(primaryKeyValue, currentPage.get(mid).get(primaryKey), primaryKeyType);

		/*
		 * This if condition is here because if we are inserting a value bigger than the
		 * maximum value, we have to exceed the last index by 1
		 */
		if (compare < 0)
			currentPage.insertElementAt(record, insertionPosition);
		else {
			insertionPosition += 1;
			currentPage.insertElementAt(record, insertionPosition);
		}

		// loading data from config file and saving max number of rows in a variable
		File appConfig = new File("src\\main\\resources\\DBApp.config");
		FileInputStream propsInput = null;
		try {
			propsInput = new FileInputStream("src\\main\\resources\\DBApp.config");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		Properties prop = new Properties();
		try {
			prop.load(propsInput);
		} catch (IOException e) {
			e.printStackTrace();
		}
		int maxRowsInPage = Integer.parseInt((String) prop.get("MaximumRowsCountinPage"));

		// if condition to check if the current page is full
		if (maxRowsInPage + 1 <= currentPage.size()) {
			// counting overflow pages of current page as well as all non-overflow pages
			int overflowPageCount = countNumberOfPageOverflows(tableName, pageNumber);
			int nonOverflowPageCount = countNumberOfPagesWithoutOverflows(tableName);
			/*
			 * if condition to check if the current page is a non-overflow page, if so we
			 * check its overflows or next page if there is no overflows
			 */
			if (overflowNumber == 0) {
				// if condition to check if current page has overflows, if so we check them,
				// else check next page
				if (overflowPageCount == 0) { // No overflows
					// if condition to check if this is the last page, if so create a new page after
					// it
					String nextPagePath = tableName + "[" + (pageNumber + 1) + "](" + 0 + ").class";
					Vector<Hashtable<String, Object>> nextPage = new Vector();
					if (nonOverflowPageCount != pageNumber) {
						nextPage = readPageIntoVector(nextPagePath);
					}
					// if condition to check if next page is full, if full create an overflow page
					if (maxRowsInPage == nextPage.size()) {
						Vector<Hashtable<String, Object>> newOverflowPage = new Vector();

						Hashtable<String, Object> rowToShift = currentPage.remove(currentPage.size() - 1);
						newOverflowPage.add(rowToShift);

						String newOverflowPagePath = tableName + "[" + pageNumber + "](" + (1) + ").class";
						writeVectorIntoPage(newOverflowPagePath, newOverflowPage);
						writeVectorIntoPage(currentPagePath, currentPage);

						if (insertionPosition == maxRowsInPage)
							pageInsertedInto = newOverflowPagePath;
						else
							changePageNameInAllIndices(tableName, rowToShift, newOverflowPagePath);

						// in the else, page is not full, so we insert in the next page
					} else {
						Hashtable<String, Object> rowToShift = currentPage.remove(currentPage.size() - 1);
						nextPage.insertElementAt(rowToShift, 0);

						writeVectorIntoPage(nextPagePath, nextPage);
						writeVectorIntoPage(currentPagePath, currentPage);

						if (insertionPosition == maxRowsInPage)
							pageInsertedInto = nextPagePath;
						else
							changePageNameInAllIndices(tableName, rowToShift, nextPagePath);

					}
					// in this else, the page has overflows, so we check them instead of looking at
					// the next page
				} else {
					// checking if all overflow pages are full
					boolean areAllPagesFull = true;
					int i;
					String loopPagePath = null;
					Vector<Hashtable<String, Object>> loopPage = null;
					for (i = 1; i <= overflowPageCount; i++) {
						loopPagePath = tableName + "[" + pageNumber + "](" + i + ").class";
						loopPage = readPageIntoVector(loopPagePath);

						if (loopPage.size() < maxRowsInPage) {
							areAllPagesFull = false;
							break;
						}
					}
					// if pages are full, create a new overflow page
					if (areAllPagesFull) {
						i = overflowPageCount + 1;
						String newPagePath = tableName + "[" + pageNumber + "](" + i + ").class";
						Vector<Hashtable<String, Object>> newPage = new Vector();
						writeVectorIntoPage(newPagePath, newPage);
					}
					// this loop keeps shifting the rows in the pages till we reach a page that is
					// not full
					loopPagePath = currentPagePath;
					loopPage = currentPage;
					for (int j = 0; j < i; j++) {

						String nextLoopPagePath = tableName + "[" + pageNumber + "](" + (j + 1) + ").class";
						Vector<Hashtable<String, Object>> nextLoopPage = readPageIntoVector(nextLoopPagePath);

						Hashtable<String, Object> rowToShift = loopPage.remove(loopPage.size() - 1);
						nextLoopPage.insertElementAt(rowToShift, 0);

						if (insertionPosition == maxRowsInPage && j == 0)
							pageInsertedInto = nextLoopPagePath;
						else
							changePageNameInAllIndices(tableName, rowToShift, nextLoopPagePath);

						writeVectorIntoPage(loopPagePath, loopPage);
						if (nextLoopPage.size() <= maxRowsInPage) {
							writeVectorIntoPage(nextLoopPagePath, nextLoopPage);
						} else {
							loopPage = nextLoopPage;
							loopPagePath = nextLoopPagePath;
						}

					}

				}
				// this else means that the current page is an overflow page, so we will insert
				// and shift in the overflow pages
			} else {
				// again checking if all pages are full and creating a new overflow page if so
				boolean areAllPagesFull = true;
				int i;
				String loopPagePath = null;
				Vector<Hashtable<String, Object>> loopPage = null;
				for (i = overflowNumber + 1; i <= overflowPageCount; i++) {
					loopPagePath = tableName + "[" + pageNumber + "](" + i + ").class";
					loopPage = readPageIntoVector(loopPagePath);

					if (loopPage.size() < maxRowsInPage) {
						areAllPagesFull = false;
						break;
					}
				}
				if (areAllPagesFull) {
					i = overflowPageCount + 1;
					String newPagePath = tableName + "[" + pageNumber + "](" + i + ").class";
					Vector<Hashtable<String, Object>> newPage = new Vector();
					writeVectorIntoPage(newPagePath, newPage);
				}
				// again, shifting rows till we reach a page that is not full
				loopPagePath = currentPagePath;
				loopPage = currentPage;
				for (int j = overflowNumber; j < i; j++) {

					String nextLoopPagePath = tableName + "[" + pageNumber + "](" + (j + 1) + ").class";
					Vector<Hashtable<String, Object>> nextLoopPage = readPageIntoVector(nextLoopPagePath);

					Hashtable<String, Object> rowToShift = loopPage.remove(loopPage.size() - 1);
					nextLoopPage.insertElementAt(rowToShift, 0);

					if (insertionPosition == maxRowsInPage && j == overflowNumber)
						pageInsertedInto = nextLoopPagePath;
					else
						changePageNameInAllIndices(tableName, rowToShift, nextLoopPagePath);

					writeVectorIntoPage(loopPagePath, loopPage);
					if (nextLoopPage.size() <= maxRowsInPage) {
						writeVectorIntoPage(nextLoopPagePath, nextLoopPage);
					} else {
						loopPage = nextLoopPage;
						loopPagePath = nextLoopPagePath;
					}
				}
			}
			// this else means that the current page is not full, so we serialize normally
		} else
			writeVectorIntoPage(currentPagePath, currentPage);
		return pageInsertedInto;
	}

	public void writeVectorIntoPage(String pageFileName, Vector<Hashtable<String, Object>> page) {
		try {
			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\" + pageFileName);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public Grid selectSuitableGridForSelect(String tableName, HashSet<String> hashSetColumnName) {
		ArrayList<Grid> tableGrids = (ArrayList<Grid>) allIndexes.get(tableName);
		Grid bestGrid = null;
		int countMatches = 0;
		int bestcountMatches = 0;
		int bestsize = 0;

		if (tableGrids == null) {
			return null;
		}

		for (Grid grid : tableGrids) {
			for (String colName : grid.namesAndLevels.keySet()) {
				if (hashSetColumnName.contains(colName)) {
					countMatches++;
				}
			}
			if ((countMatches > bestcountMatches || (countMatches != 0 && countMatches == bestcountMatches
					&& grid.namesAndLevels.size() < bestsize))) {
				bestGrid = grid;
				bestcountMatches = countMatches;
				bestsize = grid.namesAndLevels.size();
			}
			countMatches = 0;
		}
		return bestGrid;
	}

	public void insertRecordInAllIndices(String filename, Hashtable<String, Object> htblColNameValue,
			String strTableName) {

		ArrayList<Grid> allIndicesOfCurrentTable = (ArrayList<Grid>) this.allIndexes.get(strTableName);

		if (allIndicesOfCurrentTable == null) {
			return;
		}
		// loop on all indices
		for (int i = 0; i < allIndicesOfCurrentTable.size(); i++) {
			Grid currentIndex = allIndicesOfCurrentTable.get(i);
			boolean needToSerialze = currentIndex.insertIntoGrid(htblColNameValue, filename);

			if (needToSerialze) {
				try {
					FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\allGrids\\indices.class");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(allIndexes);
					out.close();
					fileOut.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public HashSet<Hashtable<String, Object>> binarySearchPrimaryKeyAndNoIndex(ArrayList<SQLTerm> andedTerms,
			String primaryColumn, String primaryDataType, ArrayList<String> datatypes) {
		HashSet<Hashtable<String, Object>> results = new HashSet<Hashtable<String, Object>>();
		SQLTerm termForBinarySearch = null;
		boolean secondRangeBoundaryExists = false;
		SQLTerm secondBoundaryTerm = null;
		// search for a term with primary key column, = has precedence over other
		// operators
		for (SQLTerm term : andedTerms) {
			if (term._strColumnName.equals(primaryColumn) && !term._strOperator.equals("=")) {
				termForBinarySearch = term;
			} else if (term._strColumnName.equals(primaryColumn) && term._strOperator.equals("=")) {
				termForBinarySearch = term;
				break;
			}
		}
		// search for a second term with primary key column, if there is, we want to
		// check upper and lower bound
		for (SQLTerm term : andedTerms) {
			if (term._strColumnName.equals(primaryColumn) && !term._strOperator.equals("=")
					&& !term.equals(termForBinarySearch)) {
				secondBoundaryTerm = term;
				secondRangeBoundaryExists = true;
			}

		}
		int numOfPages = countNumberOfPagesWithoutOverflows(termForBinarySearch._strTableName);

		// variables for the main term
		String pageName = binarySearchOnPagesForSelect(termForBinarySearch._strTableName, numOfPages,
				termForBinarySearch._strColumnName, termForBinarySearch._objValue, primaryDataType);
		int pageNumber = getFilePageNumber(pageName);
		int overFlowNumber = getFileOverflowNumber(pageName);
		int recordIndex = binarySearchInPagesForSelect(primaryColumn, termForBinarySearch._strTableName, pageNumber,
				overFlowNumber, termForBinarySearch._objValue, primaryDataType);
		// variables for the second term, if it exists and is meaningful (both terms
		// together form a range)
		String secondaryPageName = null;
		int secondaryOverFlowNumber = -1;
		int secondaryPageNumber = -1;
		int secondaryRecordIndex = -1;
		if (secondRangeBoundaryExists) {
			if (((secondBoundaryTerm._strOperator.equals("<") || secondBoundaryTerm._strOperator.equals("<="))
					&& (termForBinarySearch._strOperator.equals(">") || termForBinarySearch._strOperator.equals(">=")))
					|| ((secondBoundaryTerm._strOperator.equals(">") || secondBoundaryTerm._strOperator.equals(">="))
							&& (termForBinarySearch._strOperator.equals("<")
									|| termForBinarySearch._strOperator.equals("<=")))) {

				secondaryPageName = binarySearchOnPagesForSelect(secondBoundaryTerm._strTableName, numOfPages,
						secondBoundaryTerm._strColumnName, secondBoundaryTerm._objValue, primaryDataType);

				secondaryPageNumber = getFilePageNumber(secondaryPageName);
				secondaryOverFlowNumber = getFileOverflowNumber(secondaryPageName);

				secondaryRecordIndex = binarySearchInPagesForSelect(primaryColumn, secondBoundaryTerm._strTableName,
						secondaryPageNumber, secondaryOverFlowNumber, secondBoundaryTerm._objValue, primaryDataType);
			}
		}
		// if the term operator is an =
		if (termForBinarySearch._strOperator.equals("=")) {
			Vector<Hashtable<String, Object>> page = readPageIntoVector(pageName);
			Hashtable<String, Object> record = page.get(recordIndex);
			for (int x = 0; x < andedTerms.size(); x++) {
				String columnName = andedTerms.get(x)._strColumnName;
				Object rowValue = record.get(columnName);
				String dataType = datatypes.get(x);
				boolean checkIfTrue = false;

				if (andedTerms.get(x)._strOperator.equals("=")) {
					checkIfTrue = (compare(andedTerms.get(x)._objValue, rowValue, dataType) == 0);
				} else if (andedTerms.get(x)._strOperator.equals("!=")) {
					checkIfTrue = (compare(andedTerms.get(x)._objValue, rowValue, dataType) != 0);
				} else if (andedTerms.get(x)._strOperator.equals("<")) {
					checkIfTrue = (compare(rowValue, andedTerms.get(x)._objValue, dataType) < 0);
				} else if (andedTerms.get(x)._strOperator.equals(">")) {
					checkIfTrue = (compare(rowValue, andedTerms.get(x)._objValue, dataType) > 0);
				} else if (andedTerms.get(x)._strOperator.equals("<=")) {
					checkIfTrue = (compare(rowValue, andedTerms.get(x)._objValue, dataType) <= 0);
				} else if (andedTerms.get(x)._strOperator.equals(">=")) {
					checkIfTrue = (compare(rowValue, andedTerms.get(x)._objValue, dataType) >= 0);
				}

				if (!checkIfTrue)
					return new HashSet<Hashtable<String, Object>>();
			}
			results.add(record);
			return results;

		} else {
			// if second term is the upper bound, and main term is lower bound
			if (secondRangeBoundaryExists
					&& (secondBoundaryTerm._strOperator.equals("<") || secondBoundaryTerm._strOperator.equals("<="))
					&& ((termForBinarySearch._strOperator.equals(">"))
							|| termForBinarySearch._strOperator.equals(">="))) {
				for (int i = pageNumber; i <= secondaryPageNumber; i++) {
					int noOfOverflows = countNumberOfPageOverflows(termForBinarySearch._strTableName, i);
					int overflowToStopAt = i == secondaryPageNumber ? secondaryOverFlowNumber : noOfOverflows;
					int overflowToBeginAt = i == pageNumber ? overFlowNumber : 0;
					for (int j = overflowToBeginAt; j <= overflowToStopAt; j++) {
						Vector<Hashtable<String, Object>> page = readPageIntoVector(
								termForBinarySearch._strTableName + "[" + i + "](" + j + ").class");
						int indexToStopAt = (i == secondaryPageNumber) && (j == secondaryOverFlowNumber)
								? secondaryRecordIndex
								: page.size() - 1;
						int indexToBeginAt = (i == pageNumber) && (j == overFlowNumber) ? recordIndex : 0;
						for (int k = indexToBeginAt; k <= indexToStopAt; k++) {
							boolean checkIfTrue = true;
							for (int x = 0; x < andedTerms.size(); x++) {
								String columnName = andedTerms.get(x)._strColumnName;
								Object rowValue = page.get(k).get(columnName);
								String dataType = datatypes.get(x);

								if (andedTerms.get(x)._strOperator.equals("=")) {
									checkIfTrue &= (compare(andedTerms.get(x)._objValue, rowValue, dataType) == 0);
								} else if (andedTerms.get(x)._strOperator.equals("!=")) {
									checkIfTrue &= (compare(andedTerms.get(x)._objValue, rowValue, dataType) != 0);
								} else if (andedTerms.get(x)._strOperator.equals("<")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) < 0);
								} else if (andedTerms.get(x)._strOperator.equals(">")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) > 0);
								} else if (andedTerms.get(x)._strOperator.equals("<=")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) <= 0);
								} else if (andedTerms.get(x)._strOperator.equals(">=")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) >= 0);
								}
							}
							if (checkIfTrue)
								results.add(page.get(k));
						}
					}
				}
				// opposite of last if
			} else if (secondRangeBoundaryExists
					&& (secondBoundaryTerm._strOperator.equals(">") || secondBoundaryTerm._strOperator.equals(">="))
					&& ((termForBinarySearch._strOperator.equals("<"))
							|| termForBinarySearch._strOperator.equals("<="))) {
				for (int i = secondaryPageNumber; i <= pageNumber; i++) {
					int noOfOverflows = countNumberOfPageOverflows(termForBinarySearch._strTableName, i);
					int overflowToStopAt = i == pageNumber ? overFlowNumber : noOfOverflows;
					int overflowToBeginAt = i == secondaryPageNumber ? secondaryOverFlowNumber : 0;
					for (int j = overflowToBeginAt; j <= overflowToStopAt; j++) {
						Vector<Hashtable<String, Object>> page = readPageIntoVector(
								termForBinarySearch._strTableName + "[" + i + "](" + j + ").class");
						int indexToStopAt = (i == pageNumber) && (j == overFlowNumber) ? recordIndex : page.size() - 1;
						int indexToBeginAt = (i == secondaryPageNumber) && (j == secondaryOverFlowNumber)
								? secondaryRecordIndex
								: 0;
						for (int k = indexToBeginAt; k <= indexToStopAt; k++) {
							boolean checkIfTrue = true;
							for (int x = 0; x < andedTerms.size(); x++) {
								String columnName = andedTerms.get(x)._strColumnName;
								Object rowValue = page.get(k).get(columnName);
								String dataType = datatypes.get(x);

								if (andedTerms.get(x)._strOperator.equals("=")) {
									checkIfTrue &= (compare(andedTerms.get(x)._objValue, rowValue, dataType) == 0);
								} else if (andedTerms.get(x)._strOperator.equals("!=")) {
									checkIfTrue &= (compare(andedTerms.get(x)._objValue, rowValue, dataType) != 0);
								} else if (andedTerms.get(x)._strOperator.equals("<")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) < 0);
								} else if (andedTerms.get(x)._strOperator.equals(">")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) > 0);
								} else if (andedTerms.get(x)._strOperator.equals("<=")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) <= 0);
								} else if (andedTerms.get(x)._strOperator.equals(">=")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) >= 0);
								}
							}
							if (checkIfTrue)
								results.add(page.get(k));
						}
					}
				}
				// no range is formed, so we check from beginning to upperbound
			} else if (termForBinarySearch._strOperator.equals("<") || termForBinarySearch._strOperator.equals("<=")) {
				for (int i = 1; i <= pageNumber; i++) {
					int noOfOverflows = countNumberOfPageOverflows(termForBinarySearch._strTableName, i);
					int overflowToStopAt = i == pageNumber ? overFlowNumber : noOfOverflows;
					for (int j = 0; j <= overflowToStopAt; j++) {
						Vector<Hashtable<String, Object>> page = readPageIntoVector(
								termForBinarySearch._strTableName + "[" + i + "](" + j + ").class");
						int indexToStopAt = (i == pageNumber) && (j == overFlowNumber) ? recordIndex : page.size() - 1;
						for (int k = 0; k <= indexToStopAt; k++) {
							boolean checkIfTrue = true;
							for (int x = 0; x < andedTerms.size(); x++) {
								String columnName = andedTerms.get(x)._strColumnName;
								Object rowValue = page.get(k).get(columnName);
								String dataType = datatypes.get(x);

								if (andedTerms.get(x)._strOperator.equals("=")) {
									checkIfTrue &= (compare(andedTerms.get(x)._objValue, rowValue, dataType) == 0);
								} else if (andedTerms.get(x)._strOperator.equals("!=")) {
									checkIfTrue &= (compare(andedTerms.get(x)._objValue, rowValue, dataType) != 0);
								} else if (andedTerms.get(x)._strOperator.equals("<")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) < 0);
								} else if (andedTerms.get(x)._strOperator.equals(">")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) > 0);
								} else if (andedTerms.get(x)._strOperator.equals("<=")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) <= 0);
								} else if (andedTerms.get(x)._strOperator.equals(">=")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) >= 0);
								}
							}
							if (checkIfTrue)
								results.add(page.get(k));
						}
					}
				}
				// searching from lower bound to end
			} else if (termForBinarySearch._strOperator.equals(">") || termForBinarySearch._strOperator.equals(">=")) {
				for (int i = pageNumber; i <= numOfPages; i++) {
					int noOfOverflows = countNumberOfPageOverflows(termForBinarySearch._strTableName, i);
					int overflowToBeginAt = i == pageNumber ? overFlowNumber : 0;
					for (int j = overflowToBeginAt; j <= noOfOverflows; j++) {
						Vector<Hashtable<String, Object>> page = readPageIntoVector(
								termForBinarySearch._strTableName + "[" + i + "](" + j + ").class");
						int indexToStartAt = (i == pageNumber) && (j == overFlowNumber) ? recordIndex : 0;
						for (int k = indexToStartAt; k < page.size(); k++) {
							boolean checkIfTrue = true;
							for (int x = 0; x < andedTerms.size(); x++) {
								String columnName = andedTerms.get(x)._strColumnName;
								Object rowValue = page.get(k).get(columnName);
								String dataType = datatypes.get(x);

								if (andedTerms.get(x)._strOperator.equals("=")) {
									checkIfTrue &= (compare(andedTerms.get(x)._objValue, rowValue, dataType) == 0);
								} else if (andedTerms.get(x)._strOperator.equals("!=")) {
									checkIfTrue &= (compare(andedTerms.get(x)._objValue, rowValue, dataType) != 0);
								} else if (andedTerms.get(x)._strOperator.equals("<")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) < 0);
								} else if (andedTerms.get(x)._strOperator.equals(">")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) > 0);
								} else if (andedTerms.get(x)._strOperator.equals("<=")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) <= 0);
								} else if (andedTerms.get(x)._strOperator.equals(">=")) {
									checkIfTrue &= (compare(rowValue, andedTerms.get(x)._objValue, dataType) >= 0);
								}
							}
							if (checkIfTrue)
								results.add(page.get(k));
						}
					}
				}

			}
		}
		return results;
	}

	public String binarySearchOnOverflowPagesForSelect(String tableName, int totalNumberOfOverflowPages,
			String primaryKey, Object primaryKeyValue, String primaryKeyType, int page) {
		int start = 0;
		int end = totalNumberOfOverflowPages;
		while (start <= end) {
			int mid = (start + end) / 2;
			// here after getting the middle page of the overflow pages of this specific
			// page we get its path
			// getting this over flow page to check if its in the range of this overflow
			// page
			Vector<Hashtable<String, Object>> currentPage = readPageIntoVector(
					tableName + "[" + page + "](" + mid + ").class");

			// max value of this overflow page
			Object maxValue = currentPage.get(currentPage.size() - 1).get(primaryKey);
			// min value of this overflow page
			Object minValue = currentPage.get(0).get(primaryKey);

			// comparing the primary key value to the minimum value
			int compareToMinValue = compare(primaryKeyValue, minValue, primaryKeyType);
			if (compareToMinValue == 0) {
				return tableName + "[" + page + "](" + mid + ").class";
			}
			// if the primary value < minimum value we take the half before the mid
			else if (compareToMinValue < 0) {
				end = mid - 1;
			} else {
				// else we compare it to the max value
				int compareToMaxValue = compare(primaryKeyValue, maxValue, primaryKeyType);
				if (compareToMaxValue == 0) {
					return tableName + "[" + page + "](" + mid + ").class";
				}
				// if it is less than the maximum value then its in in this over flow page
				else if (compareToMaxValue < 0) {
					return tableName + "[" + page + "](" + mid + ").class";
				}
				// if it is greater than the max val then we have two cases
				else {
					// if it is less than the min of the next page
					// then it has to be inserted in this current page
					int next = mid + 1;
					if (next <= totalNumberOfOverflowPages) { // if we are not in the last page
						Vector<Hashtable<String, Object>> nextPage = readPageIntoVector(
								tableName + "[" + page + "](" + next + ").class");
						Object minOfNextPage = nextPage.get(0).get(primaryKey);
						int compareToMinOfNextPage = compare(primaryKeyValue, minOfNextPage, primaryKeyType);
						if (compareToMinOfNextPage < 0) {
							// insert in the this page
							return tableName + "[" + page + "](" + mid + ").class";
						} else if (compareToMinOfNextPage == 0) {
							return tableName + "[" + page + "](" + next + ").class";
						} else {
							// if it is not less than the minimum of the next page then we continue binary
							// search
							start = mid + 1;
						}
					} else {
						// if we are already in the last page and the key is greater than its maximum
						// then it should
						// be inserted in this last page
						return tableName + "[" + page + "](" + mid + ").class";
					}
				}
			}
		}
		return "";
	}

	public String binarySearchOnPagesForSelect(String tableName, int totalNumberOfPages, String primaryKey,
			Object primaryKeyValue, String primaryKeyType) {
		int start = 1;
		int end = totalNumberOfPages;

		// checking if the value of the input is less than
		// the minimum value in the table and returning the first page
		Vector<Hashtable<String, Object>> firstPage = readPageIntoVector(tableName + "[1](0).class");
		Object firstPageMin = firstPage.get(0).get(primaryKey);
		int compareWithFirstPageMin = compare(primaryKeyValue, firstPageMin, primaryKeyType);
		if (compareWithFirstPageMin < 0) {
			return tableName + "[1](0).class";
		}

		// Binary Search to get the page
		while (start <= end) {
			int mid = (start + end) / 2;
			Vector<Hashtable<String, Object>> currentPage = readPageIntoVector(tableName + "[" + mid + "](0).class");
			Object maxValue = null;

			// Getting the minimum value of the page
			Object minValue = currentPage.get(0).get(primaryKey);

			// Getting the number of overflow pages of this page
			int numberOfOverFlows = countNumberOfPageOverflows(tableName, mid);

			// If the numberOfOverFlows is zero then the maximum value is the maximum of
			// current page
			if (numberOfOverFlows == 0) {
				maxValue = currentPage.get(currentPage.size() - 1).get(primaryKey);
			} else {
				// if it's not 0 then we get the maximum value from the last overflow page
				Vector<Hashtable<String, Object>> lastOverflowPage = readPageIntoVector(
						tableName + "[" + mid + "](" + numberOfOverFlows + ").class");
				maxValue = lastOverflowPage.get(lastOverflowPage.size() - 1).get(primaryKey);
			}
			// we compare the primary value to the minimum value
			int compareToMinValue = compare(primaryKeyValue, minValue, primaryKeyType);
			if (compareToMinValue == 0) {
				return tableName + "[" + mid + "](0).class";
			} else if (compareToMinValue < 0) {
				// if it is less than the minimum value then we take the part before the
				// current/middle page
				end = mid - 1;
			} else if (compareToMinValue > 0) {
				// else if it is greater than the minimum
				// compare the primary value to the max value
				int compareToMaxValue = compare(primaryKeyValue, maxValue, primaryKeyType);
				if (compareToMaxValue == 0) {
					return tableName + "[" + mid + "](0).class";
				} else if (compareToMaxValue > 0) {
					// If it is grater than the maximum value then we have two cases

					// First Case:
					// if it is less than the minimum of the next page then we know it has to
					// be inserted in the last overflow of the current page(if exists)
					int next = mid + 1;
					// if we are not already on the last page
					if (next <= totalNumberOfPages) {
						Vector<Hashtable<String, Object>> nextPage = readPageIntoVector(
								tableName + "[" + next + "](0).class");
						Object minofnextpage = nextPage.get(0).get(primaryKey);
						int compareToMinOfNextPage = compare(primaryKeyValue, minofnextpage, primaryKeyType);
						if (compareToMinOfNextPage < 0) {
							// it must be inserted at the end of the last overflow of this page
							return tableName + "[" + mid + "](" + numberOfOverFlows + ").class";
						} else if (compareToMinOfNextPage == 0) {
							// if it is equal to the minimum of the next page then we found it
							return tableName + "[" + next + "](0).class";
						} else {
							// or if its is not then we take the first half of the pages
							start = mid + 1;
						}
					} else {
						// if we are already on the last page(no next page) and the key is greater than
						// the maximum
						// return last overflow of the current page which is the last page
						return tableName + "[" + mid + "](" + numberOfOverFlows + ").class";
					}
				} else if (compareToMaxValue < 0) {
					// if the primary value is less than the maximum then its in this page or one of
					// its overflows
					// if there is no overflows we return this page
					if (numberOfOverFlows == 0) {
						return tableName + "[" + mid + "](0).class";
					} else {
						// else we binary search on the page and its overflows to find the correct page
						String overFlowResult = binarySearchOnOverflowPagesForSelect(tableName, numberOfOverFlows,
								primaryKey, primaryKeyValue, primaryKeyType, mid);
						return overFlowResult;
					}
				}
			}
		}

		return "";
	}

	public int binarySearchInPagesForSelect(String primaryKey, String tableName, int pageNumber, int overflowNumber,
			Object primaryKeyValue, String primaryKeyType) {
		// set current page path according to parameters and then read into a vector
		String currentPagePath = tableName + "[" + pageNumber + "](" + overflowNumber + ").class";
		Vector<Hashtable<String, Object>> currentPage = readPageIntoVector(currentPagePath);

		// binary search to find insertion position
		int startValue = 0;
		int endValue = currentPage.size() - 1;
		int mid = 0;
		while (startValue <= endValue) {
			mid = (startValue + endValue) / 2;
			int compare = compare(primaryKeyValue, currentPage.get(mid).get(primaryKey), primaryKeyType);
			if (compare == 0)
				return mid;
			else if (compare < 0) {
				endValue = mid - 1;
			} else {
				startValue = mid + 1;
			}
		}
		return mid;
	}

	public HashSet<Hashtable<String, Object>> loadAll(String tableName) {
		HashSet<Hashtable<String, Object>> results = new HashSet<>();
		int noOfPages = countNumberOfPagesWithoutOverflows(tableName);
		for (int i = 1; i <= noOfPages; i++) {
			int noOfOverflows = countNumberOfPageOverflows(tableName, i);
			for (int j = 0; j <= noOfOverflows; j++) {
				Vector<Hashtable<String, Object>> page = readPageIntoVector(tableName + "[" + i + "](" + j + ").class");
				for (int k = 0; k < page.size(); k++) {
					results.add(page.get(k));
				}
			}
		}
		return results;
	}

}
