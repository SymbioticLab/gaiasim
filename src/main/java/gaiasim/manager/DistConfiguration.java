package gaiasim.manager;

// The configuration parser

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class DistConfiguration {

    protected String masterIP;

    protected String [] agentIPs;
    protected int numAgents;

    protected String configFilePath;
    public DistConfiguration(int numAgents, String configFile) {
        this.numAgents = numAgents;
        this.configFilePath = configFile;
        agentIPs = new String[numAgents];

        parseFile(configFile);

    }

    private void parseFile(String configFile) {
        System.out.println("Loading configuration from " + configFile);

        try {
            FileReader fr = new FileReader(configFile);
            BufferedReader br = new BufferedReader(fr);

            int cnt = 0;
            String line;
            while ((line = br.readLine()) != null) {

                if (line.trim().length() == 0){
                    continue; // ignoring empty lines
                }

                if (cnt == 0){
                    masterIP = line;
                } else {
                    agentIPs [cnt - 1] = line;
                }

                cnt++;
            }

            if ( cnt != numAgents + 1){ // Num = ctrl + agents
                System.err.println("ERROR: corrupted config file");
                System.exit(1);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public String getAgentIP(int index) { return agentIPs[index]; }

    public String getMasterIP() { return masterIP; }

    public int getNumAgents() { return numAgents; }
}
