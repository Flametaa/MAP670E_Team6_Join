package project;
/**
 * This class was implemented in order built  some  methods needed to write and read data to and
from “.csv” files. This class contains somes methods to save modifications and the creating of buffers 
for represent to the blocks(very important for the implementation de NLJ and BNLJ)
**/
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class Relations_File {
	
	public static boolean isEmptyFile(String filename) {
		boolean empty = true;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			if (br.readLine() != null) {
				empty = false;
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return empty;
	}
	
	
	public static boolean fileExists(String filepath) {
		File file = new File(filepath);
		if(file.exists()) {
			return true;
		}
		return false;
	}
	
	public static void deleteFile(String file) {
		Path filepath = Paths.get(file);
		try {
			Files.deleteIfExists(filepath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void renameFile(String filepath, String newpath) {
		Path source = Paths.get(filepath);
		Path newdir = Paths.get(newpath);
		try {
			Files.move(source, newdir,
					StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	// deletes the result file if exists, from a previous run
	public static void createNewFile(String newFile) {
		File outputResultFile = new File(newFile);
		try {
			// first, delete the file if already exists from another run
			outputResultFile.delete();
			// then create the file
			outputResultFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void writeHeader(BufferedWriter bw, Tuple tuple) {
		try {
			bw.write(tuple.getHeader() + "\n");
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void writeRelationToFile(List<Tuple> relation, String outputFile) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
			for (int i=0; i<relation.size(); i++) {
				bw.write(relation.get(i).getCSVFile() + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	// Reads a csv file that represents a relation and returns the number of records.
	// The first line of the csv file should contain the number of records.
	public static int getNumberRows(String csvfile) {
		int number_of_tuples = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(csvfile));
			String line = br.readLine();
			number_of_tuples = Integer.parseInt(line);
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return number_of_tuples;
	}

	
	public static int countLinesFile(String filename) {
		int lines = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			while (br.readLine() != null) {
				lines++;
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}
	
	public static void writeTuple(BufferedWriter bw, Tuple tuple) {
		try {
			// writer that appends to the file
			bw.write(tuple.getCSVFile() + "\n");
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Tuple getNextTuple(BufferedReader br, String relationName) {
		Tuple tuple = null;
		try {
			String line = br.readLine();
			if (line != null) {
				String[] attributes = line.split(",");
				tuple = new Tuple(attributes.length, relationName);
				
				for (int i=0; i<attributes.length; i++) {
					String quotes = "\"";
					String withoutQuotes = attributes[i].replaceAll(quotes,"");
					String attributeValue = withoutQuotes;
					String attributeName = relationName + i;
					Relation_Attribute attribute = new Relation_Attribute(attributeValue, attributeName);
					tuple.attributes.add(attribute);
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return tuple;
	}
	
	public static String getName(String csvfile) {
		File file = new File(csvfile);
		return file.getName().split(".csv")[0];
	}
	
	

	public static BufferedReader createBufferedReader(String csvfile) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(csvfile));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return br;
	}
	
	
	public static BufferedReader createBufferedReaderAndSkipFirstLine(String csvfile) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(csvfile));
			br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return br;
	}
	
	
	public static void closeBufferedReader(BufferedReader br) {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	
	
	public static void resetBufferedReader(BufferedReader br) {
		try {
			br.reset();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static BufferedWriter createBufferedWriter(String csvfile) {
		BufferedWriter bw = null;
		try {
			// writer that appends to the file
			bw = new BufferedWriter(new FileWriter(csvfile, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bw;
	}
	public static void closeBufferedWriter(BufferedWriter bw) {
		try {
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void printRelation(List<Tuple> relation) {
		System.out.println(relation.get(0).getHeader());
		for (int i=0; i<relation.size(); i++) {
			System.out.println(relation.get(i));
		}
		System.out.println();
	}
	


}
