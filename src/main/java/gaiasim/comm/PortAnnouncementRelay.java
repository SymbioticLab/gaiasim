package gaiasim.comm;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.comm.PortAnnouncementMessage;
import gaiasim.network.NetGraph;

// Relays PortAnnouncementMessages from SendingAgents
// to the OpenFlow controller that will create FlowMods
// to set up paths.
public class PortAnnouncementRelay {

    public NetGraph net_graph_;
    public LinkedBlockingQueue<PortAnnouncementMessage> port_announcements_;
    
    public PortAnnouncementRelay(NetGraph net_graph,
                                 LinkedBlockingQueue<PortAnnouncementMessage> port_announcements) {
        net_graph_ = net_graph;
        port_announcements_ = port_announcements;
    }

    public void relay_ports() {
        int num_ports_recv = 0;
        String announcement;
       
        try {
            // Set up the fifo that we'll receive from
            Runtime rt = Runtime.getRuntime();
            String recv_fifo_name = "/tmp/gaia_fifo_to_ctrl";
            rt.exec("rm " + recv_fifo_name).waitFor();

            // Set up the fifo that we'll write to
            File f = new File("/tmp/gaia_fifo_to_of");
            FileWriter fw = new FileWriter(f);
            BufferedWriter bw = new BufferedWriter(fw);

            // Set the file permissions on the fifo to 666 (anyone rw) because
            // the emulated controller is run as root (so that it can be started
            // from mininet). If we don't set the file permissions here, the
            // fifo can only be accessed as root.
            rt.exec("mkfifo " + recv_fifo_name + " -m 666").waitFor();

            // Send the number of port announcements that will be
            // sent to the OF controller.
            // TODO: Currently we tell the OF controller that it will only
            //       receive total_num_paths messages. However, each path
            //       will require more than one FlowMod to be installed,
            //       so we'll actually send more than total_num_paths messages.
            //       The actual number will be closer to the sum of the
            //       number of hops in each path we're announcing. Need to
            //       figure out a way around this (maybe send a metadata with
            //       a path_id before each hop announcement. Something like
            //       <path_id, num_hops>. There will be a total of total_num_paths_
            //       such metadata announcements).
            bw.write(Integer.toString(net_graph_.total_num_paths_) + '\n');
            bw.flush();

            // As we receive port announcements, send the information needed
            // by the OF controller to set FlowMods for the paths.
            while (num_ports_recv < net_graph_.total_num_paths_) {
                PortAnnouncementMessage m = port_announcements_.take();
                announcement = "Received port <" + m.sa_id_ + ", " + m.ra_id_ + ", " + m.path_id_ + ", " + m.port_no_ + ">";
                // TODO: Set up forwarding rule based on this path
                num_ports_recv++;
                bw.write(announcement + '\n');
                bw.flush();
                System.out.println(announcement);
            }
            bw.close();

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
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

}
