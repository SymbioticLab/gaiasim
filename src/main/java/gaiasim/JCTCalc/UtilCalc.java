package gaiasim.JCTCalc;

import com.opencsv.CSVReader;
import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.spark.DAGReader;
import gaiasim.spark.Job;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class UtilCalc {

    private HashMap<String, Job> jobs_;
    private NetGraph net_graph_;

    private double WINDOW_START = 500;
    private double WINDOW_END = 2000;

    ArrayList<Job> completed_jobs_ = new ArrayList<>();
    ArrayList<Coflow> completed_coflows_ = new ArrayList<>();
    ArrayList<Job> jobs_by_time_ = new ArrayList<>();

    HashMap<String, CCTInfo> cctInfoHashMap = new HashMap<>();
    Queue<Coflow> toBeScheduledQueue = new LinkedList<>();

    private int count = 0;
    private long dev = 0;

    public UtilCalc(String gml_file, String trace_file, double window_start, double window_end, String csv) {

        WINDOW_START = window_start;
        WINDOW_END = window_end;

        try {
            this.net_graph_ = new NetGraph(gml_file, 1);
            // if we don't compare across workload, we can set workload_factor to 1
            jobs_ = DAGReader.read_trace_new(trace_file, net_graph_, 1);
            parseCCTCSV(csv);

            // Create sorted vector of jobs
            for (String id : jobs_.keySet()) {
                jobs_by_time_.add(jobs_.get(id));
            }
            Collections.sort(jobs_by_time_, new Comparator<Job>() {
                public int compare(Job o1, Job o2) {
                    if (o1.start_time_ == o2.start_time_) return 0;
                    return o1.start_time_ < o2.start_time_ ? -1 : 1;
                }
            });


        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void calc() {
        double totalVolume = 0;

        // traverse all coflows to see which coflows finish within the window
        for (Job j : jobs_by_time_) {
            for (Coflow cf : j.coflows_.values()) {


                if (cctInfoHashMap.get(cf.id_).getEndTime() > WINDOW_END || cctInfoHashMap.get(cf.id_).getEndTime() < WINDOW_START) {
                    continue;
                }

                for (Flow f : cf.flows_.values()) {
                    if (!f.src_loc_.equals(f.dst_loc_)) {

                        totalVolume += f.volume_;
                    }
                }

            }

        }

//        System.err.println("Total volume: " + totalVolume);

        System.err.println(String.format("%10f", totalVolume));

    }

    private void parseCCTCSV(String csvFile) throws IOException {

        File file = new File(csvFile);
        FileReader fReader = new FileReader(file);
        CSVReader csvReader = new CSVReader(fReader);

        String[] header = csvReader.readNext();

        List<String[]> content = csvReader.readAll();

        double startOffset = Double.MAX_VALUE;

        // first find the start time and subtract all timestamps
        for (String[] line : content) {
            double currentStartTimestamp = Double.parseDouble(line[1]);
            if (currentStartTimestamp < startOffset) {
                startOffset = currentStartTimestamp;
            }

        }

        for (String[] line : content) {
            double curStart = Double.parseDouble(line[1]);
            double curEnd = Double.parseDouble(line[2]);
            double curDuration = Double.parseDouble(line[3]);
            cctInfoHashMap.put(line[0], new CCTInfo(curStart - startOffset, curEnd - startOffset, curDuration));
        }
    }
}
