import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SortMergeJoin {
	private Table r;
	private Table l;
	private PageManager PageManagerR;
	private int PageinR;
	private PageManager PageManagerL;
	private int PageinL;
	private int RightPointer;
	private int LeftPointer;
	private int markRecord;
	private int markPage;
	private List<Record> joined;
	private Comparator<TableRecord> comparator;
	
	public SortMergeJoin(Table t1, Table t2) {
		String pathSortedR = "database/sorted_tables/sorted_" + t1.getTablename() + ".csv";
		String pathSortedL = "database/sorted_tables/sorted_" + t2.getTablename() + ".csv";
		File fileR = new File(pathSortedR);
		File fileL = new File(pathSortedL);
		if (!fileR.exists()) {
			SortOperator sortOperatorR = new SortOperator(t1);
			sortOperatorR.externalSort("database/runR", "database/mergeR", pathSortedR);
		}
		if (!fileL.exists()) {
			SortOperator sortOperatorL = new SortOperator(t2);
			sortOperatorL.externalSort("database/runR", "database/mergeL", pathSortedL);
		}
		this.r= new Table("sorted_" + t1.getTablename(), pathSortedR);
		this.l= new Table("sorted_" + t2.getTablename(), pathSortedL);
		this.PageManagerR=new PageManager(r);
		this.PageManagerL=new PageManager(l);
		this.PageinR=this.PageManagerR.getNumPages();
		this.PageinL=this.PageManagerL.getNumPages();
		this.RightPointer=0;
		this.LeftPointer=0;
		this.markRecord=-1;
		this.markPage=-1;
		this.joined = new ArrayList<Record>();
		this.comparator = (r1, r2) -> (r1.getValue(0)).compareTo(r2.getValue(0));
	}
	
	public Table join(String tablename, String filename) {
		int right=0;
		boolean end=false;
		boolean lock=true;
		List<TableRecord> PageR= PageManagerR.loadPageToMemory(right);
		for ( int x=0 ; x<this.PageinL;x++) { // Looping over all the left pages starting from zero
			this.LeftPointer=0; // in Every left page we start with a left pointer at position zero 
			lock=true; // lock is used when we need to turn to another Left Page
			List<TableRecord> PageL= PageManagerL.loadPageToMemory(x);  // Locating the LeftPage
			while(end==false && lock==true) { //Keep running if it's not done , and when left page still have records
				if(this.markRecord==-1 && this.markPage==-1) {
					while(comparator.compare(PageL.get(LeftPointer), PageR.get(RightPointer)) < 0 && lock) {
						LeftPointer++; // Increment the left pointer 
						if(LeftPointer > PageL.size()-1) { // But we need to check if this pointer is bigger than the Page size
							lock=false; // If true we need to move to the next left page thus apply lock so it wont enter another function down
						}
					}
					while(comparator.compare(PageL.get(LeftPointer), PageR.get(RightPointer)) > 0 && lock) {
						RightPointer++;
						if(RightPointer > PageR.size()-1) { // we need to check if the rightpointer is bigger than the size of the right page
							right++; // increment the page
							if(right>this.PageinR-1) { // but also check if the page exist
								end=true; //if no we end the program
							} else {
								PageR=PageManagerR.loadPageToMemory(right); // if yes we call this page
								RightPointer=0; // and set the pointer to zero
							}
						}
					}
					if(lock) {
						this.markPage=right; // set the mark page
						this.markRecord=RightPointer; // set the mark record
					}
				}
				if(comparator.compare(PageL.get(LeftPointer), PageR.get(RightPointer)) == 0 && lock) {
					// merge both
					List<String> resultList = new ArrayList<String>(Arrays.asList(PageL.get(LeftPointer).getValues()));
					resultList.addAll(Arrays.asList(PageR.get(RightPointer).getValues()));
					String[] result = resultList.toArray(new String[0]);
					Record r = new Record(result);
					joined.add(r);
					RightPointer++;
					if(RightPointer > PageR.size()-1) { // same as before 
						right++;
						if(right>this.PageinR-1) {
							end=true;
						} else {
							PageR=PageManagerR.loadPageToMemory(right);
							RightPointer=0;
						}
	
					}
					//return result
				}
				else {
					if(lock==true) {
						RightPointer=this.markRecord; // reset the pointers
						right=this.markPage; // reset the page
						PageR=PageManagerR.loadPageToMemory(right); // we reload the page we now need 
						
						LeftPointer++;
						if(LeftPointer > PageL.size()-1) {
							lock=false;
						}
						this.markRecord=-1;
						this.markPage=-1;
					}
					
				}
			}
		}
	DiskManager.writeRecordsToDisk(filename, joined);
	Table joinedTable = new Table(tablename, filename);
	return joinedTable;
	}
}
