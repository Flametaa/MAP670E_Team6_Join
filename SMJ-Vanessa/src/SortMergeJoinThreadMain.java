import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SortMergeJoinThreadMain extends Thread {
	Table table_r, table_s;
	int nb_threads;
	String output_path;
	long duration_partition, duration_threads, duration_combine;

	public SortMergeJoinThreadMain(Table t1, Table t2, int nbThreads, String filename) {
		this.table_r = t1;
		this.table_s = t2;
		this.nb_threads = nbThreads;
		this.output_path = filename;
	}

	@Override
	public void run() {
		File directory = new File("database/partitions");
		if (!directory.exists())
			DiskManager.createDirectory(directory.getPath());
		directory = new File("database/merged_partitions");
		if (!directory.exists())
			DiskManager.createDirectory(directory.getPath());

		long start_time;
		try {
			start_time = System.currentTimeMillis();
			createPartitions();
			duration_partition = System.currentTimeMillis() - start_time;
		} catch (IOException ex) {
			Logger.getLogger(SortMergeJoinThreadMain.class.getName()).log(Level.SEVERE, null, ex);
			return;
		}

		start_time = System.currentTimeMillis();
		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < nb_threads; i++) {
			Table r = new Table("r_" + Integer.toString(i), "database/partitions/r_" + Integer.toString(i) + ".csv");
			Table s = new Table("s_" + Integer.toString(i), "database/partitions/s_" + Integer.toString(i) + ".csv");
			Thread thread = new Thread(
					new SortMergeJoinThread(r, s, "database/merged_partitions/" + Integer.toString(i) + ".csv"));
			thread.start();
			threads.add(thread);
		}
		threads.forEach((thread) -> {
			try {
				thread.join();
			} catch (InterruptedException ex) {
				Logger.getLogger(SortMergeJoinThreadMain.class.getName()).log(Level.SEVERE, null, ex);
			}
		});

		for (int i = 0; i < nb_threads; i++) {
			DiskManager.deleteFromDisk("database/sorted_tables/sorted_r_" + Integer.toString(i) + ".csv");
			DiskManager.deleteFromDisk("database/sorted_tables/sorted_s_" + Integer.toString(i) + ".csv");
		}

		duration_threads = System.currentTimeMillis() - start_time;

		start_time = System.currentTimeMillis();
		DiskManager.mergePartitionsOnDisk("database/merged_partitions/", output_path, nb_threads);
		duration_combine = System.currentTimeMillis() - start_time;

		DiskManager.deleteFromDisk("database/partitions");
		DiskManager.deleteFromDisk("database/merged_partitions");
	}

	private void createPartitions() throws IOException {
		TreeMap<Integer, Integer> histo_r = getExactHistogram(table_r);
		Integer[] boundaries = getBoundaries(histo_r, nb_threads);
		partition(table_r, boundaries, JoinSide.R);
		partition(table_s, boundaries, JoinSide.S);
	}

	enum JoinSide {
		R, S
	}

	private TreeMap<Integer, Integer> getExactHistogram(Table table) {
		TreeMap<Integer, Integer> histo = new TreeMap<>();
		List<Record> records = table.getAllRecords();
		for (Record r : records) {
			Integer val = Integer.parseInt(r.getValue(0));
			histo.put(val, histo.containsKey(val) ? histo.get(val) + 1 : 1);
		}
		return histo;
	}

	private Integer[] getBoundaries(TreeMap<Integer, Integer> histogram, int nbBins) {
		int nb_records_per_thread = (histogram.size() - 1) / nbBins + 1;
		int sum = 0;
		int x = nb_records_per_thread, current_bin = 0;
		Integer[] boundaries = new Integer[nbBins - 1];
		for (Map.Entry<Integer, Integer> entry : histogram.entrySet()) {
			sum += entry.getValue();
			if (sum >= x) {
				boundaries[current_bin] = entry.getKey();
				current_bin++;
				if (current_bin == nbBins - 1) {
					break;
				}
				x += nb_records_per_thread;
			}
		}
		return boundaries;
	}

	private void partition(Table table, Integer[] boundaries, JoinSide side) {
		String str = side == JoinSide.R ? "database/partitions/r_" : "database/partitions/s_";
		DiskManager.splitPartitionsOnDisk(table.getFilename(), str, nb_threads, boundaries);
	}
}
