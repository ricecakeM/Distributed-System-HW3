/**
 * A command line parser class
 */
public class CommandLineParser {
    private final int NUM_ARGS = 5;
    private final int MAX_THREADS = 1024;
    private final int MAX_SKIERS = 100000;
    private final int MIN_LIFTS = 5;
    private final int MAX_LIFTS = 60;
    private final int MAX_RUNS = 20;
    private Integer numThreads = 0;
    private Integer numSkiers = 0;
    private Integer numLifts = 0;
    private Integer numRuns = 0;
    private String serverAddress;

    public CommandLineParser() {}

    /**
     * Parse the command line args
     * @return Boolean
     */
    public boolean parseCommandLine(String[] args) {
        // check the number of args
        if (args.length != NUM_ARGS) {
            System.out.println("The number of arguments is incorrect");
            return false;
        }
        try {
            this.numThreads = Integer.parseInt(args[0]);
            if (this.numThreads > MAX_THREADS) {
                System.out.println("Maximum number of threads allowed is 1024.");
                return false;
            }

            this.numSkiers = Integer.parseInt(args[1]);
            if (this.numSkiers > MAX_SKIERS) {
                System.out.println("Maximum number of skiers allowed is 100000.");
                return false;
            }

            this.numLifts = Integer.parseInt(args[2]);
            if (this.numLifts < MIN_LIFTS || this.numLifts > MAX_LIFTS) {
                System.out.println("The number of lifts should be between 5 and 60.");
                return false;
            }

            this.numRuns = Integer.parseInt(args[3]);
            if (this.numRuns > MAX_RUNS) {
                System.out.println("Maximum mean number of ski lifts is 20.");
                return false;
            }

            this.serverAddress = args[4];
        } catch (final NumberFormatException e) {
            System.out.println("Cannot parse arguments correctly.");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public int getNumThreads() {
        return this.numThreads;
    }

    public int getNumSkiers() {
        return this.numSkiers;
    }

    public int getNumLifts() {
        return this.numLifts;
    }

    public int getNumRuns() {
        return this.numRuns;
    }

    public String getServerAddress() {
        return this.serverAddress;
    }
}