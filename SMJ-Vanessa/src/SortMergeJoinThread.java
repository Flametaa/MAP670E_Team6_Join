/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author AbdelRahman
 */
public class SortMergeJoinThread extends SortMergeJoin implements Runnable
{
    String output_path;
    public SortMergeJoinThread(Table t1, Table t2, String filename)
    {
        super(t1, t2);
        this.output_path = filename;
    }

    @Override
    public void run()
    {
        this.join(output_path);
    }
    
}
