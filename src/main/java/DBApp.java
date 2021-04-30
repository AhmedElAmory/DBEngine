import java.io.*;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
//602


public class DBApp implements DBAppInterface {
	int counter=0;
	// Constructor
	public DBApp() {
		init();
	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {

		if (!new File("src\\main\\resources\\data").exists()) {
			// Creating a File object
			File file = new File("src\\main\\resources\\data");
			// Creating the directory
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
		System.out.println(counter++);
		// check if columns in hashtable exist in metadata and are of correct datatypes
//////////check date-time input constraintss///////////////////////////////////////////////////////////
//////////make sure of min max constraints and size and that they are the same as name-type hashtable
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
							"src\\main\\resources\\data\\" + strTableName + "[1](0)" + ".class");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(newPage);
					out.close();
					fileOut.close();
				} catch (IOException i) {
					i.printStackTrace();
				}
			} else {
				String primaryKey = "";
				String primarykeyType = "";
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
								primaryKey = arr[1];
								primarykeyType = arr[2];
							}
						}
						current=br.readLine();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				Object primarykeyvalue = htblColNameValue.get(primaryKey);

				String file = binarySearchOnPages(strTableName, numberOfpages, primaryKey, primarykeyvalue,
						primarykeyType);
				

				binarySearchAndInsertInPages(primaryKey, strTableName, getFilePageNumber(file),
						getFileOverflowNumber(file), primarykeyvalue, primarykeyType, htblColNameValue);

			}
		}
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the rows to update.
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		checkUpdateConstraints(strTableName, strClusteringKeyValue, htblColNameValue);
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();
			Object clusteringKeyValue = "";
			String clusteringKeyType = "";
			String clusteringKeyColumnName = "";
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

			String location = getPageAndIndex(strTableName, clusteringKeyValue, clusteringKeyColumnName,
					clusteringKeyType);

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

			// call method with tableName,ClusteringKey name,Clustering key value,
			// Clustering key type,and hashtable
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue entries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		// Check constraints
		String PrimaryKey = checkDeleteConstraints(strTableName, htblColNameValue);

		// Check if PrimaryKey exists
		if (PrimaryKey.equals("")) {
			// Do linear search
			linearDeleteOnTable(strTableName, htblColNameValue);
		} else {
			// If primarykey exists then we will delete only one unique row so we can use
			// binary search as the column is sorted
			// Do binary search
			String[] primarykeyData = PrimaryKey.split(" ");

			String pageToDeleteFrom = binaryDeleteFromTable(strTableName, htblColNameValue, primarykeyData[0],
					primarykeyData[1]);

			if (!(pageToDeleteFrom.equals(""))) {
				Vector<Hashtable<String, Object>> pageVector = readPageIntoVector(pageToDeleteFrom);
				int start = 0;
				int end = pageVector.size() - 1;

				// Binary Search to get the page
				while (start <= end) {
					int mid = (start + end) / 2;

					int comparison = compare(pageVector.get(mid).get(primarykeyData[0]),
							htblColNameValue.get(primarykeyData[0]), primarykeyData[1]);
					if (comparison == 0) {
						pageVector.remove(mid);
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
							if (pageVector.size() == 0) { // If the page is empty (size of vector==0), we delete it
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
											File oldfile = new File("src\\main\\resources\\data\\" + strTableName + "["
													+ x + "](" + k + ").class");
											File newfile = new File("src\\main\\resources\\data\\" + strTableName + "["
													+ temp + "](" + k + ").class");
											oldfile.renameTo(newfile);
										}
									}
								} else {
									// If Yes(it did have overflows), we shift only the overflows to the left one
									// step
									for (int l = 1; l <= TotalNumberOfOverflowPages; l++) {

										File oldfile = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
												+ "](" + l + ").class");
										int temp = l - 1;
										File newfile = new File("src\\main\\resources\\data\\" + strTableName + "[" + i
												+ "](" + temp + ").class");
										oldfile.renameTo(newfile);
									}
								}
							}
						} else { // it is an overflow page
							if (pageVector.size() == 0) { // If the page is empty (size of vector==0), we delete it
								File f1 = new File(
										"src\\main\\resources\\data\\" + strTableName + "[" + i + "](" + j + ").class"); // file
																															// to
																															// be
																															// deleted
								f1.delete();

								// Decrement all overflow pages with overflow number > j
								for (int x = j + 1; x <= TotalNumberOfOverflowPages; x++) {
									File f2 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i + "]("
											+ x + ").class");
									int temp = x - 1;
									File f3 = new File("src\\main\\resources\\data\\" + strTableName + "[" + i + "]("
											+ temp + ").class");
									f2.renameTo(f3);
								}
							}
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
		}
	}

	// following method creates one index – either multidimensional
	// or single dimension depending on the count of column names passed.
	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {

	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		return null;
	}
	// Supplement methods

	// This method check if the inserted table already exists.
	public boolean checkTableExists(String tableName) {
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
									new Date(Integer.parseInt(minDates[0]), Integer.parseInt(minDates[1]),
											Integer.parseInt(minDates[2])),
									arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]),
									new Date(Integer.parseInt(maxDates[0]), Integer.parseInt(maxDates[1]),
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
									new Date(Integer.parseInt(minDates[0]), Integer.parseInt(minDates[1]),
											Integer.parseInt(minDates[2])),
									arr[2]);
							int compareToMax = compare(htblColNameValue.get(arr[1]),
									new Date(Integer.parseInt(maxDates[0]), Integer.parseInt(maxDates[1]),
											Integer.parseInt(maxDates[2])),
									arr[2]);

//							if (compareToMin < 0) {
//								throw new DBAppException(
//										"The value inserted in column " + arr[1] + " is below the minimum value");
//							}
//							if (compareToMax > 0) {
//								throw new DBAppException(
//										"The value inserted in column " + arr[1] + " is above the maximum value");
//							}
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

	public String getFileTableName(String fileName) {
		String tableName = "";
		int c = 0;
		while (fileName.charAt(c) != '[') {
			tableName += fileName.charAt(c);
			c++;
		}
		return tableName;
	}

	public int getFilePageNumber(String fileName) {
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

	public int getFileOverflowNumber(String fileName) {
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
								tableName + "[" + page + "](" + mid + ").class");
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

	// comparing objects
	public int compare(Object obj1, Object obj2, String primaryKeyType) {

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
			return ((String) obj1).compareTo((String) obj2);
		}
		return -100;
	}

	// count the pages for a specific table
	public int countNumberOfPagesWithoutOverflows(String Tablename) {
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
	public int countNumberOfPageOverflows(String Tablename, int Number) {

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

	public Vector<Hashtable<String, Object>> readPageIntoVector(String pageName) {
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

	//// Delete Methodssssss

	public void linearDeleteOnTable(String strTableName, Hashtable<String, Object> htblColNameValue) {

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
						Page.remove(row);
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
						File f1 = new File(
								"src\\main\\resources\\data\\" + strTableName + "[" + i + "](" + j + ").class"); // file
																													// to
																													// be
																													// deleted
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

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue entries are ANDED together

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
		} else {
			throw new DBAppException("No row where this primary key exists!");
		}
	}

	public void binarySearchAndInsertInPages(String primaryKey, String tableName, int pageNumber, int overflowNumber,
			Object primaryKeyValue, String primaryKeyType, Hashtable<String, Object> record) throws DBAppException {
		//set current page path according to parameters and then read into a vector
		String currentPagePath = tableName + "[" + pageNumber + "](" + overflowNumber + ").class";
		Vector<Hashtable<String, Object>> currentPage = readPageIntoVector(currentPagePath);

		//binary search to find insertion position
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
		//inserting according to binary search
		int insertionPosition = mid;
		int compare = compare(primaryKeyValue, currentPage.get(mid).get(primaryKey), primaryKeyType);

		/*
		 * This if condition is here because if we are inserting a value bigger than the
		 * maximum value, we have to exceed the last index by 1
		 */
		if (compare < 0)
			currentPage.insertElementAt(record, insertionPosition);
		else
			currentPage.insertElementAt(record, insertionPosition + 1);

		//loading data from config file and saving max number of rows in a variable
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

		//if condition to check if the current page is full
		if (maxRowsInPage + 1 <= currentPage.size()) {
			//counting overflow pages of current page as well as all non-overflow pages
			int overflowPageCount = countNumberOfPageOverflows(tableName, pageNumber);
			int nonOverflowPageCount = countNumberOfPagesWithoutOverflows(tableName);
			/*
			 * if condition to check if the current page is a non-overflow page, if so we
			 * check its overflows or next page if there is no overflows
			 */
			if (overflowNumber == 0) {
				//if condition to check if current page has overflows, if so we check them, else check next page
				if (overflowPageCount == 0) {
					//if condition to check if this is the last page, if so create a new page
					if (nonOverflowPageCount == pageNumber) {
						String newPagePath = tableName + "[" + (pageNumber + 1) + "](" + 0 + ").class";
						Vector<Hashtable<String, Object>> newPage = new Vector();
						writeVectorIntoPage(newPagePath, newPage);
					}
					//set the next page path and vector
					String nextPagePath = tableName + "[" + (pageNumber + 1) + "](" + 0 + ").class";
					Vector<Hashtable<String, Object>> nextPage = readPageIntoVector(nextPagePath);

					//if condition to check if next page is full, if full create an overflow page
					if (maxRowsInPage == nextPage.size()) {
						Vector<Hashtable<String, Object>> newPage = new Vector();
						newPage.add(currentPage.get(currentPage.size() - 1));
						currentPage.remove(currentPage.size() - 1);
						String newPagePath = tableName + "[" + pageNumber + "](" + (overflowPageCount + 1) + ").class";
						writeVectorIntoPage(newPagePath, newPage);
						writeVectorIntoPage(currentPagePath, currentPage);
					}
					//in the else, page is not full, so we insert in the next page
					else {
						nextPage.insertElementAt(currentPage.get(currentPage.size() - 1), 0);
						writeVectorIntoPage(nextPagePath, nextPage);
						currentPage.remove(currentPage.size() - 1);
						writeVectorIntoPage(currentPagePath, currentPage);
					}
				}
				//in this else, the page has overflows, so we check them instead of looking at the next page
				else {
					//checking if all overflow pages are full
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
					//if pages are full, create a new overflow page
					if (areAllPagesFull) {
						i = overflowPageCount + 1;
						String newPagePath = tableName + "[" + pageNumber + "](" + i + ").class";
						Vector<Hashtable<String, Object>> newPage = new Vector();
						writeVectorIntoPage(newPagePath, newPage);
					}
					//this loop keeps shifting the rows in the pages till we reach a page that is not full
					loopPagePath = currentPagePath;
					loopPage = currentPage;
					for (int j = 0; j < i; j++) {

						String nextLoopPagePath = tableName + "[" + pageNumber + "](" + (j + 1) + ").class";
						Vector<Hashtable<String, Object>> nextLoopPage = readPageIntoVector(nextLoopPagePath);

						nextLoopPage.insertElementAt(loopPage.get(loopPage.size() - 1), 0);
						loopPage.remove(loopPage.size() - 1);

						writeVectorIntoPage(loopPagePath, loopPage);
						if (nextLoopPage.size() <= maxRowsInPage)
							writeVectorIntoPage(nextLoopPagePath, nextLoopPage);
						else {
							loopPage = nextLoopPage;
							loopPagePath = nextLoopPagePath;
						}
					}
				}
			}
			//this else means that the current page is an overflow page, so we will insert and shift in the overflow pages
			else {
				//again checking if all pages are full and creating a new overflow page if so
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
				if (areAllPagesFull) {
					i = overflowPageCount + 1;
					String newPagePath = tableName + "[" + pageNumber + "](" + i + ").class";
					Vector<Hashtable<String, Object>> newPage = new Vector();
					writeVectorIntoPage(newPagePath, newPage);
				}
				//again, shifting rows till we reach a page that is not full
				loopPagePath = currentPagePath;
				loopPage = currentPage;
				for (int j = overflowNumber; j < i; j++) {

					String nextLoopPagePath = tableName + "[" + pageNumber + "](" + (j + 1) + ").class";
					Vector<Hashtable<String, Object>> nextLoopPage = readPageIntoVector(nextLoopPagePath);

					nextLoopPage.insertElementAt(loopPage.get(loopPage.size() - 1), 0);
					loopPage.remove(loopPage.size() - 1);

					writeVectorIntoPage(loopPagePath, loopPage);
					if (nextLoopPage.size() <= maxRowsInPage)
						writeVectorIntoPage(nextLoopPagePath, nextLoopPage);
					else {
						loopPage = nextLoopPage;
						loopPagePath = nextLoopPagePath;
					}
				}
			}
		}
		//this else means that the current page is not full, so we serialize normally
		else
			writeVectorIntoPage(currentPagePath, currentPage);
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

}
