# Nested Loop Join and Block Nested Join Implementation

This folder or branch contains our implementations of the **Nested Loop Join(simple NLJ) and Block Nested Loop Join algorithm(BNLJ)**. 

> All data is found in the data folder named **database_created** which contains five files(these files contains only **int records** R1,R2,R3,R4,R5)and 2 files (autrhors and posts which contain **strings records or values**) in format .csv .
> All the code is found in the **src/project**. It contains the single-threaded implementation about NLJ and BNLJ.
>

## A. Description of Structure:

> To achieve our final goal this implementation was divided in the following sections or class:

### 1. Class Main.java:
Using Main.java would allow both implementations to run on existing data sets in .csv format. It would return the result as .csv results or the filename the user specified. To run the ''main'' class it is necessary to enter the inputs or arguments in the format of a list of strings in the following format:

Format of 'args': -R database_created/out.csv -c1 0 -S database_created/posts.csv -c2 0 -t BNLJ -n_tup_block 200 -o results.csv

Hence:
* -R: Refers to the path of the first table or relation 
* -S: Refers to the path of the second table or relation. 
* -c1: Refers to the column number of the first table to realize the equi-join.
* -c2: Refers to the column number of the second table to realize the equi-join.
* -t: Refers to the type of algorithm (NLJ or BNLJ in capital letters) chosen to build the equi-join.
* -n_tup_block : Specifies the number of tuples per block when executing the BNLJ.
* -o: Specifies the output of the attached table. To do so, we have to put the name in which we want to save it (in our case we consider the name 'results.csv').

Then during execution it will also display the total execution time used by the type of algorithm and the memory space used to execute the joint.

### 2.Relations_File:
This class contains all the necessary methods and operations to write and read data from ".csv" files (which contain records or values in integer or string format of each table or relationship). This class allows us to obtain the name of each input relationship, the number of rows or tuples that are in the first line of the .csv file, the values or records of each relationship by browsing each tuple. Also this class has the buffers methods to read and write tuples in each block when executing the BNLJ algorithm.

### 3.Relation_Attribute:
This class refers to the columns of a dotn relation where the input arguments are the values and the name of the column corresponding to each relation.

### 4.Tuple:

This class was created to contain methods on the use of tuples or lines in our chosen relationships. The arguments or inputs of this class are the number of attributes (number of records per tuple) and the name of the relation in which they belong.

This class also contains the tuple join method which allows to update the values in the output join relationship using the classic join or equi-join to perform operations in NLJ and BNLJ.

### 5.NLJ:
 It contains the implementation of the Simple Nested Loop Join. For the realization of this implementation we have as a starting point the readings of the websites:
-http://web.cs.ucla.edu/classes/fall14/cs143/notes/join
-https://www.geeksforgeeks.org/join-algorithms-in-database/#:~:text=There%20are%20two%20algorithms%20to,and%20occupies%20BR%20blocks.
-https://www.sciencedirect.com/science/article/pii/S0022000014001536

### 6.BNLJ:
 It contains the implementation of the Block Nested Loop Join (BNLJ) algorithm. 
The Block Nested Loop Join algorithm is the most used because the smallest relationship is chosen to be iterated in the
To do this, we had to understand the notions of blocks for the two relationships when applying the Block Nested Loop Join algorithm.

In order to achieve this, we have developed the following websites as a reference point:

- https://www.geeksforgeeks.org/join-algorithms-in-database/#:~:text=There%20are%20two%20algorithms%20to,and%20occupies%20BR%20blocks.
- http://info.usherbrooke.ca/llavoie/enseignement/Modules/BD043-Optimisation-Elmasri_6e_Ch19_rLL.pdf
- https://courses.cs.washington.edu/courses/cse444/10au/lectures/lecture20.pdf





