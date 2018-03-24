import java.io.*;
import java.util.*;

public class Main {

    private static final String RELATION1 = "bag1";
    private static final String RELATION2 = "bag2";
    private static final String BAG_DIFFERENCE = "bagDifference";

    private static final int numOfTuplesPerBlock = 40;
    private static final int numOfBytesPerTuple = 320;

    private static int numOfBlocksForMemory = 0;
    private static int bufferSize = 0;

    private static int sortBlockReads = 0;
    private static int sortBlockWrites = 0;
    private static int blockReads = 0;
    private static int blockWrites = 0;

    private static int totalOutputTuples = 0;


    public static void main(String[] args) {
        File tempDir = new File("temp");
        tempDir.mkdir();

        long start = Calendar.getInstance().getTimeInMillis();

        System.out.println("Started at: " + Calendar.getInstance().getTime());
        String temp = performSort();
        long diff = Calendar.getInstance().getTimeInMillis() - start;
        System.out.println("Time taken for sorting: " + ((double) diff / (1000)) + " seconds, " + ((double) diff / (1000*60)) + " minutes");
        System.out.println("Number of block reads: " + sortBlockReads);
        System.out.println("Number of block writes: " + sortBlockWrites);
        System.out.println("Total number of IOs for sort: " + (sortBlockWrites + sortBlockReads));
        System.out.println("\n\n");


        System.out.println("calculating bag difference...");
        calculateBagDifference(temp.split(":")[0], temp.split(":")[1]);
        System.out.println("Finished at: " + Calendar.getInstance().getTime());


        diff = Calendar.getInstance().getTimeInMillis() - start;
        System.out.println("Time taken: " + ((double) diff / (1000)) + " seconds, " + ((double) diff / (1000*60)) + " minutes");
        System.out.println("Number of block reads: " + blockReads);
        System.out.println("Number of block writes: " + blockWrites);
        System.out.println("Total disk IOs: " + (blockReads + blockWrites));

        System.out.println("Total number of tuples in output: " + totalOutputTuples);
        System.out.println("Number of blocks used for output: " + ((int) Math.ceil((double) totalOutputTuples / numOfTuplesPerBlock)));

    }

    private static void calculateBagDifference(String p1, String p2) {
        try {
            String file1 = "temp/" + RELATION1 + "-sublist-" + p1 + "-0";
            String file2 = "temp/" + RELATION2 + "-sublist-" + p2 + "-0";

            BufferedReader reader1 = new BufferedReader(new FileReader(file1));
            BufferedReader reader2 = new BufferedReader(new FileReader(file2));

            Iterator<String> iterator1 = getBlockFromFile(reader1).iterator();
            Iterator<String> iterator2 = getBlockFromFile(reader2).iterator();

            List<String> output = new ArrayList<>();

            boolean flag1 = true;
            boolean flag2 = true;

            String a = null, b = null;
            while (iterator1.hasNext() && iterator2.hasNext()) {
                if (flag1) a = iterator1.next();
                if (flag2) b = iterator2.next();

                if (a.split("~>")[0].compareTo(b.split("~>")[0]) < 0) {
                    flag1 = true;
                    flag2 = false;
                    int count = Integer.valueOf(a.split("~>")[1]);
                    if (count > 0) output.add(a.split("~>")[0] + ": " + count);
                } else if (b.split("~>")[0].compareTo(a.split("~>")[0]) < 0) {
                    flag2 = true;
                    flag1 = false;
                } else {
                    int count = Integer.valueOf(a.split("~>")[1]) - Integer.valueOf(b.split("~>")[1]);
                    if (count > 0) output.add(a.split("~>")[0] + ": " + count);
                    else output.add(a.split("~>")[0] + ": 0");
                    flag1 = true;
                    flag2 = true;
                }

                if (!iterator1.hasNext()) {
                    iterator1 = getBlockFromFile(reader1).iterator();
                }
                if (!iterator2.hasNext()) {
                    iterator2 = getBlockFromFile(reader2).iterator();
                }

                if (output.size() == numOfTuplesPerBlock) {
                    totalOutputTuples += numOfTuplesPerBlock;
                    blockWrites ++;
                    writeToFile(output, "temp/" + BAG_DIFFERENCE);
                }
            }

            while (iterator1.hasNext()) {
                String line = iterator1.next();
                int count = Integer.valueOf(line.split("~>")[1]);
                if (count > 0) output.add(line.split("~>")[0] + ": " + count);
                if (output.size() == numOfTuplesPerBlock) {
                	totalOutputTuples += numOfTuplesPerBlock;
                	blockWrites++;
                	writeToFile(output, "temp/" + BAG_DIFFERENCE);   
                }
            }

            if (!output.isEmpty()) {
                totalOutputTuples += output.size();
                blockWrites++;
                writeToFile(output, "temp/" + BAG_DIFFERENCE); 
            }
        } catch (IOException e) {
            System.out.println("Error while calculating bag difference");
            e.printStackTrace();
        }
    }

    private static List<String> getBlockFromFile(BufferedReader reader) throws IOException {
        blockReads++;
        List<String> output = new ArrayList<>();
        String line;
        for (int i = 0; i < numOfTuplesPerBlock; i++) {
            line = reader.readLine();
            if (line == null) break;
            output.add(line);
        }
        return output;
    }

    private static void calculateBufferSize() {
        long availableMemory = Helper.getAvailableMemory();
        System.out.println("Available memory: " + availableMemory + "Bytes, " + availableMemory/(1024*1024) + "MB");
        numOfBlocksForMemory = (int) Math.floor(availableMemory / (numOfTuplesPerBlock * numOfBytesPerTuple));
        bufferSize = numOfBlocksForMemory - 1;
    }

    private static String performSort() {
        System.out.println("Sorting relation 1...");
        int p1 = (performSortFor(RELATION1));
        System.out.println("Sorting relation 2...");
        int p2 = (performSortFor(RELATION2));
        return p1 + ":" + p2;
    }

    private static int performSortFor(String relation) {
        // phase 1
        int numOfRuns = createSortedSubList(relation);
        // phase 2
        int pass = 1;
        while (numOfRuns > 1) {
            numOfRuns = mergeSubLists(relation, numOfRuns, pass);
            pass++;
        }

        return pass - 1;
    }

    private static int mergeSubLists(String relation, int numOfRuns, int pass) {
        int temp = 0;
        int run = 0;
        try {
            calculateBufferSize();
            for (; ; ++run) {
                int size = Math.min(bufferSize, numOfRuns - (run * bufferSize));
                List<List<String>> chunks = new ArrayList<>();
                List<Integer> indices = new ArrayList<>();
                List<Integer> blockNum = new ArrayList<>();
                List<String> output = new ArrayList<>();

                List<BufferedReader> readers = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    File file = new File("temp/" + relation + "-sublist-" + (pass - 1) + "-" + ((run * bufferSize) + i));
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    getNextBlockFromReader(reader, chunks, indices, blockNum, i);
                    readers.add(reader);
                }

                String selection = null;
                String prevSelection = null;
                int selectionCount = 0;

                while (true) {
                    boolean flag = false;
                    prevSelection = selection;
                    selection = null;
                    for (int i = 0; i < size; i++) {
                        if (indices.get(i) < chunks.get(i).size()) {
                            if (selection == null ||
                                    selection.compareTo(chunks.get(i).get(indices.get(i)).split("~>")[0]) > 0) {
                                selection = chunks.get(i).get(indices.get(i)).split("~>")[0];
                            }
                            flag = true;
                        } else {
                            if (getNextBlockFromReader(readers.get(i), chunks, indices, blockNum, i)) {
                                if (selection == null ||
                                        selection.compareTo(chunks.get(i).get(indices.get(i)).split("~>")[0]) > 0) {
                                    selection = chunks.get(i).get(indices.get(i)).split("~>")[0];
                                }
                                flag = true;
                            }
                        }
                    }

                    if (prevSelection != null && !prevSelection.equals(selection)) {
                        output.add(prevSelection + "~>" + selectionCount);
                        selectionCount = 0;
                        if (output.size() == numOfTuplesPerBlock) {
                            sortBlockWrites++;
                            blockWrites++;
                            writeToFile(output, relation, pass, run);
                        }
                    }

                    for (int i = 0; i < size; i++) {
                        if (indices.get(i) < chunks.get(i).size()) {
                            if (selection.equals(chunks.get(i).get(indices.get(i)).split("~>")[0])) {
                                selectionCount += Integer.valueOf(chunks.get(i).get(indices.get(i)).split("~>")[1]);
                                indices.set(i, indices.get(i) + 1);
                            }
                        }
                    }

                    if (!flag) {
                        if (!output.isEmpty()) {
                            sortBlockWrites++;
                            blockWrites++;
                            writeToFile(output, relation, pass, run);
                        }
                        break;
                    }
                }

                temp += size;
                if (temp >= numOfRuns) break;

                for (int i = 0; i < readers.size(); i++) {
                    if (readers.get(i) != null) readers.get(i).close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return run + 1;
    }

    private static boolean getNextBlockFromReader(BufferedReader reader, List<List<String>> chunks, List<Integer> indices,
                                                  List<Integer> blockNum, int index) throws IOException {
        blockReads++;
        sortBlockReads++;
        boolean temp = false;

        if (blockNum.size() == index) {
            indices.add(0);
            blockNum.add(0);
            chunks.add(new ArrayList<>());
        } else {
            chunks.get(index).clear();
        }

        for (int i = 0; i < numOfTuplesPerBlock; i++) {
            String line = reader.readLine();
            if (line == null) break;
            chunks.get(index).add(line);
        }

        if (!chunks.get(index).isEmpty()) {
            temp = true;
            indices.set(index, 0);
        }

        blockNum.set(index, blockNum.get(index) + 1);

        return temp;
    }

    private static int createSortedSubList(String relation) {
        calculateBufferSize();
        try {
            BufferedReader reader = new BufferedReader(new FileReader("data/" + relation));
            List<String> tuples = new ArrayList<>();
            String line;
            int numOfRuns = 0;
            for (int i = 0; (line = reader.readLine()) != null; i++) {
                tuples.add(line + "~>1");
                if (i == (numOfTuplesPerBlock * bufferSize) - 1) {
                    quickSort(tuples,0,tuples.size()-1);
                    i = -1;

                    writeToFile(tuples, relation, 0, numOfRuns);

                    numOfRuns++;

                    	sortBlockWrites+=bufferSize;
                        blockWrites+=bufferSize;
                        blockReads+=bufferSize;
                        sortBlockReads+=bufferSize;
                }
            }

            if (!tuples.isEmpty()) {
               	quickSort(tuples,0,tuples.size()-1);
                writeToFile(tuples, relation, 0, numOfRuns);
                numOfRuns++;
                sortBlockWrites++;
                sortBlockReads++;
                blockReads++;
                blockWrites++;
            }

            reader.close();
            return numOfRuns;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static void writeToFile(List<String> output, String relation, int pass, int run) throws IOException {
        writeToFile(output, "temp/" + relation + "-sublist-" + pass + "-" + run);
    }

    private static void writeToFile(List<String> output, String filePath) throws IOException {
        BufferedWriter writer = new BufferedWriter(
                new FileWriter(filePath, true));
        for (String string : output) {
            writer.write(string + System.getProperty("line.separator"));
        }
        writer.close();
        output.clear();
    }
  
  	private static void quickSort(List<String> a, int left, int right) {
         		int index = partition(a, left, right);
         		if (left < index - 1)
               		quickSort(a, left, index - 1);
         		if (index < right)
               		quickSort(a, index, right);
  	   }

     private static int partition(List<String> a, int left, int right) {
  		int i = left, j = right;
  	       String pivot = a.get((left + right) / 2);
  	       while (i <= j) {
  	               while (a.get(i).compareTo(pivot) < 0)
  	                   i++;
  	    	   	   while (a.get(j).compareTo(pivot) > 0)
  	                   j--;
  	             if (i <= j) {
  	                   Collections.swap(a, i, j);
  	                   i++;
  	                   j--;
  	             }
  	       };
  	       return i;
           }
}