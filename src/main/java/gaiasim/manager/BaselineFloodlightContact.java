package gaiasim.manager;

// This class sends messages to Floodlight controller

import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;

import java.io.*;
import java.net.Socket;

public class BaselineFloodlightContact {
    NetGraph netGraph; // NetGraph does not include CTRL node.
//    Socket socToFloodlight; // not using socket because not supported by mininet

    public BaselineFloodlightContact(NetGraph net_Graph) throws IOException {
        this.netGraph = net_Graph;
//        this.socToFloodlight = new Socket("127.0.0.1" , 23456);

    }

    // For each switch, iterate through all destinations (nodes), get netGraph.apsp[switch][dst], get the out_port from the Pathway.
    public void setFlowRules() throws IOException, InterruptedException {

        // Set up the fifo that we'll receive from
        Runtime rt = Runtime.getRuntime();
        String recv_fifo_name = "/tmp/gaia_fifo_to_ctrl";
        rt.exec("rm " + recv_fifo_name).waitFor();

        // Set up the fifo that we'll write to
        File f = new File("/tmp/gaia_fifo_to_of");
        FileWriter fw = new FileWriter(f);
        PrintWriter pw = new PrintWriter(fw , true);

        // Set the file permissions on the fifo to 666 (anyone rw) because
        // the emulated controller is run as root (so that it can be started
        // from mininet). If we don't set the file permissions here, the
        // fifo can only be accessed as root.
        rt.exec("mkfifo " + recv_fifo_name + " -m 666").waitFor();


        // first send metadata
        System.out.println("BaselineFloodlightContact: Sending metadata");
        int numSw = netGraph.nodes_.size(); // no need to setup the rules for CTRL.
        int numNode = numSw;

        pw.println( numSw + " " + numNode); // metadata msg

        for (int i = 0 ; i < numSw ; i++ ){
            for (int j = 0 ; j < numNode ; j++ ){
                // FIXME: hard coded IP here
                // TODO verify the mapping here
                // [src] [dstIP] [outPort]
                // dpID = src + 1
                // dstIP = "10.0.0." + (dst + 1)
                // outPort = interfaces_.get(src).get(dst);

                int src = i + 1; // src == the ID of the switch
                String dstIP = "10.0.0." + (j+1);
                String nextNode = getNextNodeFromShortestPath( src , j+1 );

                // get the interface from src to the next
                // this is only the interface number, i.e., the "1" of "eth1"
                String outPort = netGraph.interfaces_.get( String.valueOf(src) ).get( nextNode );

                String msg = src + " " + dstIP + " " + outPort;
                pw.println(msg);
                System.out.println(msg);
            }
        } // end of sending messages
        pw.close();
        fw.close();

        // We've sent all of the rules that the OF controller needs to process.
        // We must wait for the OF controller to finish setting rules before
        // continuing on. Open up another fifo to wait for a '1' from the
        // OF controller.
        f = new File(recv_fifo_name);
        FileReader fr = new FileReader(f);
        int status = fr.read();

        if (status != '1') {
            System.out.println("ERROR: Received unexpected return status " + status + " from OF controller");
            System.exit(1);
        }
        System.out.println("All rules set");
        fr.close();


/*        // wait for ACK before returning
        InputStreamReader reader = new InputStreamReader(socToFloodlight.getInputStream());
        int status = reader.read();

        if (status != '1') {
            System.out.println("ERROR: Received unexpected return status " + status + " from OF controller");
            System.exit(1);
        }
        System.out.println("All rules set");
        reader.close();*/

    }

    // TODO: we may need one when the emulation is done.
    public void unSetFlowRules(){

    }

    // src and dst starts with 1 here, as in NetGraph.
    private String getNextNodeFromShortestPath(int src , int dst){

        // get the next node on the path
        Pathway path = new Pathway(netGraph.apsp_[src][dst]);
        assert (path.node_list_.size() >= 1);
        return path.node_list_.get(1);
    }
}
