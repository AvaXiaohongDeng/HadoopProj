package readwrite;

public class ExecuteReadWrite {
    public static void main(String args[]){
        //new creates a new class/ object called WriteToHadoop based
        // on the output of writeToHadoop(args[0]) but this may raise an
        // exception error, therefore we catch the exception with catch and
        // what's inside {} will be printed to let the code run
        try {
            new WriteToHadoop().writeToHadoop(args[0]);
        } catch (Exception e) {
            System.out.println("You encountered a IO exception while writing");
        }

        try {
            new ReadFromHadoop().readFromFile(args[0]);
        } catch (Exception e) {
            System.out.println("You encountered a IO exception while reading");
        }
    }
}
