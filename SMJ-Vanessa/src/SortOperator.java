import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class SortOperator {
	public static int NUM_BUFFERS = 5;
	
	private Table table;
	private PageManager pageManager;
	private Comparator<Record> comparator;
	
	public SortOperator(Table table) {
		this.table = table;
		this.pageManager = new PageManager(this.table);
		this.comparator = (r1, r2) -> (r1.getValue(0)).compareTo(r2.getValue(0));
	}
	
	public void externalSort(String runsDir, String mergeRunsDir, String outputPath) {
		sort(runsDir);
		String result = merge(runsDir, mergeRunsDir);
		try {
			Files.copy(Paths.get(result), Paths.get(outputPath), StandardCopyOption.REPLACE_EXISTING);
			DiskManager.deleteDirectory(runsDir);
			DiskManager.deleteDirectory(mergeRunsDir);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void sort(String runsDir) {
		DiskManager.createDirectory(runsDir);
		int runsNum = (pageManager.getNumPages()-1)/NUM_BUFFERS + 1;
		for (int r=0; r<runsNum; ++r) {
			List<Record> buffersRecords = new ArrayList<Record>();
			if (r==runsNum-1) {
				int remainingPages = pageManager.getNumPages() - (runsNum-1)*NUM_BUFFERS;
				for (int p=0; p < remainingPages; ++p) {
					buffersRecords.addAll(pageManager.loadPageToMemory(r*NUM_BUFFERS + p));
				}
			} else {
				for (int p=0; p < NUM_BUFFERS; ++p) {
					buffersRecords.addAll(pageManager.loadPageToMemory(r*NUM_BUFFERS + p));
				}	
			}
			buffersRecords.sort(comparator);
			String filename = "run_" + r + ".csv";
			DiskManager.writeRecordsToDisk(runsDir + "/" + filename, buffersRecords);
		}
	}
	
	public String merge(String sortRunsDir, String mergeRunsDir) {
            DiskManager.createDirectory(mergeRunsDir);
            int sortRunsNum = (pageManager.getNumPages()-1)/NUM_BUFFERS + 1;
            int mergeRunsNum = (int)Math.ceil(Math.log(sortRunsNum)/Math.log(NUM_BUFFERS-1));
            int runsToMergeTotalNum = sortRunsNum;
            int runsNum;
            int level=0;
            do {
                
            	runsNum = (runsToMergeTotalNum-1)/(NUM_BUFFERS-1) + 1;
            	
                // <editor-fold defaultstate="collapsed" desc=" do the merge runs ">
                for (int i = 0; i < runsNum; i++) {                    
                    List<PageManager> tablesPM = new ArrayList<>();
                    
                    int nbRunsToMerge = i==runsNum-1 ? runsToMergeTotalNum-(runsNum-1)*(NUM_BUFFERS-1) : NUM_BUFFERS-1;
                    
                    // <editor-fold defaultstate="collapsed" desc=" handles the case when there's only one run to be merged ">
                    if (nbRunsToMerge == 1) {
                        String path;
                        if (level == 0) {
                            path = sortRunsDir + "/" + "run_" + (runsToMergeTotalNum - 1) + ".csv";
                        }
                        else {
                            path = mergeRunsDir + "/" + "run_" + (level - 1) + "_" + (runsToMergeTotalNum - 1) + ".csv";
                        }
                        String outputPath = mergeRunsDir + "/" + "run_" + (level) + "_" + (runsToMergeTotalNum - 1) + ".csv";
                        
                        try {
                        	Files.move(Paths.get(path), Paths.get(outputPath), StandardCopyOption.REPLACE_EXISTING);
						} catch (Exception e) {
							e.printStackTrace();
						}                        
                    }

// </editor-fold>

                    // <editor-fold defaultstate="collapsed" desc=" get page managers for the k sorted tables to be merged ">
                    for (int j = 0; j < nbRunsToMerge; j++) {
                        String path;
                        if (level == 0) {
                            path = sortRunsDir + "/" + "run_" + (i * (NUM_BUFFERS - 1) + j) + ".csv";
                        } else {
                            path = mergeRunsDir + "/" + "run_" + (level - 1) + "_" + (i * (NUM_BUFFERS - 1) + j) + ".csv";
                        }
                        tablesPM.add(new PageManager(new Table("", path)));
                    }
// </editor-fold>
                    
                    ArrayList<Iterator<Record>> records = new ArrayList<>();
                    Queue<Pair<Record, Integer>> pq = new PriorityQueue<Pair<Record, Integer>>(((o1, o2) -> comparator.compare(o1.getFirst(), o2.getFirst())));
                    int[] nbLoadedPages = new int[tablesPM.size()];

                    // <editor-fold defaultstate="collapsed" desc=" initialize the priority queue and the records iterators ">
                    for (int k = 0; k < tablesPM.size(); k++) {
                        records.add(tablesPM.get(k).loadPageToMemory(0).iterator());
                        nbLoadedPages[k] = 1;
                        pq.add(new Pair<>(records.get(k).next(), k));
                    }

// </editor-fold>

                    List<Record> buffersRecords = new ArrayList<>();
                    String outputPath = mergeRunsDir + "/" + "run_" + (level) + "_" + (i) + ".csv";

                    // <editor-fold defaultstate="collapsed" desc=" fill bufferRecords in priority order ">
                    while (pq.size() > 0) {
                        Pair<Record, Integer> nextRecord = pq.remove();
                        buffersRecords.add(nextRecord.getFirst());
                        if (buffersRecords.size() == PageManager.RECORDS_PER_PAGE) {
                            DiskManager.appendRecordsToDisk(outputPath, buffersRecords);
                            buffersRecords.clear();
                        }
                        if (records.get(nextRecord.getSecond()).hasNext()) {
                            pq.add(new Pair<>(records.get(nextRecord.getSecond()).next(), nextRecord.getSecond()));
                        } else {
                            if (nbLoadedPages[nextRecord.getSecond()] < tablesPM.get(nextRecord.getSecond()).getNumPages()) {
                                records.set(nextRecord.getSecond(), tablesPM.get(nextRecord.getSecond()).loadPageToMemory(nbLoadedPages[nextRecord.getSecond()]).iterator());
                                nbLoadedPages[nextRecord.getSecond()]++;
                                pq.add(new Pair<>(records.get(nextRecord.getSecond()).next(), nextRecord.getSecond()));
                            }
                        }
                    }
                    // </editor-fold>
// </editor-fold>  
                }
// </editor-fold>

                level++;
                runsToMergeTotalNum = runsNum;
                
            } while(runsNum!=1);
        	String result = mergeRunsDir + "/" + "run_" + (Math.max(0,mergeRunsNum-1)) + "_0.csv";
        	return result; 
        }
}
