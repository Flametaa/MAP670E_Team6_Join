
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author AbdelRahman
 */
public class SortMergeJoinThreadMain extends Thread
{
    Table table_r, table_s;
    int nb_threads;
    String output_path;
    long duration_partition, duration_threads, duration_combine;
    
    public SortMergeJoinThreadMain(Table t1, Table t2, int nbThreads, String filename)
    {
        this.table_r = t1;
        this.table_s = t2;
        this.nb_threads = nbThreads;
        this.output_path = filename;
    }
    
    @Override
    public void run()
    {
        File directory = new File("database/partitions");
        if (! directory.exists())
            directory.mkdir();
        directory = new File("database/merged_partitions");
        if (! directory.exists())
            directory.mkdir();
        
        long start_time;
        try
        {
            start_time = System.currentTimeMillis();
            createPartitions();
            duration_partition = System.currentTimeMillis() - start_time;
        }
        catch (IOException ex)
        {
            Logger.getLogger(SortMergeJoinThreadMain.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        
        start_time = System.currentTimeMillis();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < nb_threads; i++)
        {
            Table r = new Table("r_"+Integer.toString(i), "database/partitions/r_"+Integer.toString(i)+".csv");
            Table s = new Table("s_"+Integer.toString(i), "database/partitions/s_"+Integer.toString(i)+".csv");
            Thread thread = new Thread(new SortMergeJoinThread(r, s, "database/merged_partitions/"+Integer.toString(i)+".csv"));
            thread.start();
            threads.add(thread);
        }
        threads.forEach((thread) ->
        {
            try
            {
                thread.join();
            }
            catch (InterruptedException ex)
            {
                Logger.getLogger(SortMergeJoinThreadMain.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        duration_threads = System.currentTimeMillis() - start_time;
        
        
        start_time = System.currentTimeMillis();
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(output_path)))
        {
            for (int i = 0; i < nb_threads; i++)
            {
                try(BufferedReader reader = new BufferedReader(new FileReader("database/merged_partitions/"+Integer.toString(i)+".csv")))
                {
                    String line;
                    while((line=reader.readLine())!=null)
                    {
                        writer.write(line);
                        writer.write("\n");
//                        writer.newLine();
                    }
                }
            }
            writer.flush();
        }
        catch (IOException ex)
        {
            Logger.getLogger(SortMergeJoinThreadMain.class.getName()).log(Level.SEVERE, null, ex);
        }
        duration_combine = System.currentTimeMillis() - start_time;
        
        try
        {
            delete(new File("database/partitions"));
            delete(new File("database/merged_partitions"));
        }
        catch (IOException ex)
        {
            Logger.getLogger(SortMergeJoinThreadMain.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void createPartitions() throws IOException
    {
        TreeMap<Integer, Integer> histo_r = getExactHistogram(table_r);
        Integer[] boundaries = getBoundaries(histo_r, nb_threads);
        partition(table_r, boundaries, JoinSide.R);
        partition(table_s, boundaries, JoinSide.S);
    }
    
    enum JoinSide
    { R, S }

    private TreeMap<Integer, Integer> getExactHistogram(Table table) throws FileNotFoundException, IOException
    {
        TreeMap<Integer, Integer> histo = new TreeMap<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(table.getFilename())))
        {
            String line;
            while((line=reader.readLine())!=null)
            {
                Integer val = Integer.parseInt(line.substring(0, line.indexOf(Table.CSV_SPLIT_BY)).replace("\"",""));
                histo.put(val, histo.containsKey(val) ? histo.get(val)+1 : 1);
            }
        }
        return histo;
    }

    private Integer[] getBoundaries(TreeMap<Integer, Integer> histogram, int nbBins)
    {
        int nb_records_per_thread = (histogram.size() - 1)/nbBins + 1;
        int sum = 0;
        int x = nb_records_per_thread, current_bin = 0;
        Integer[] boundaries = new Integer[nbBins-1];
        for(Map.Entry<Integer, Integer> entry : histogram.entrySet())
        {
            sum += entry.getValue();
            if(sum >= x)
            {
                boundaries[current_bin] = entry.getKey();
                current_bin++;
                if (current_bin == nbBins)
                {
                    break;
                }
                x += nb_records_per_thread;
            }
        }
        return boundaries;
    }

    private void partition(Table table, Integer[] boundaries, JoinSide side) throws FileNotFoundException, IOException
    {
        String str = side == JoinSide.R ? "database/partitions/r_" : "database/partitions/s_";
        try(BufferedReader reader = new BufferedReader(new FileReader(table.getFilename())))
        {
            BufferedWriter[] writers = new BufferedWriter[nb_threads];
            for (int i = 0; i < nb_threads; i++)
            {
                writers[i] = new BufferedWriter(new FileWriter(str+Integer.toString(i)+".csv"));
            }
            
            String line;
            while((line=reader.readLine())!=null)
            {
                Integer val = Integer.parseInt(line.substring(0, line.indexOf(Table.CSV_SPLIT_BY)).replace("\"",""));
                int bin = Arrays.binarySearch(boundaries, val);
                if(bin < 0)
                    bin = -(bin + 1);
                
                writers[bin].write(line);
                writers[bin].newLine();
            }
            
            for (int i = 0; i < nb_threads; i++)
            {
                writers[i].flush();
                writers[i].close();
            }
        }
    }
    
    void delete(File f) throws IOException
    {
        if (f.isDirectory())
        {
            for (File c : f.listFiles())
            {
                delete(c);
            }
        }
        if (!f.delete())
        {
            throw new FileNotFoundException("Failed to delete file: " + f);
        }
    }
}
