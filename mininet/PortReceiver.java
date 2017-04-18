import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedReader;

public class PortReceiver {
    public static void main(String[] args) throws Exception {
        Runtime rt = Runtime.getRuntime();
        String recv_fifo_name = "/tmp/gaia_fifo_to_of";
        rt.exec("rm " +  recv_fifo_name).waitFor();
        rt.exec("mkfifo " + recv_fifo_name).waitFor();

        File f = new File(recv_fifo_name);
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        System.out.println("Connection up");
        String buf_str;
        buf_str = br.readLine();
        int num_needed = Integer.parseInt(buf_str);
        int num_recv = 0;
        System.out.println("need " + num_needed);

        String msg_id, src_ip, dst_ip;
        int num_rules, src_port, dst_port, dpid, out_port;
        boolean forward;
        while (num_recv < num_needed) {
            buf_str = br.readLine();
            String[] splits = buf_str.split(" ");
            if (splits.length != 6) {
                System.out.println("ERROR: Expected a metadata announcement but received a rule instead");
                System.out.println("Received: " + buf_str);
                System.exit(1);
            }

            msg_id = splits[0];
            num_rules = Integer.parseInt(splits[1]);
            src_ip = splits[2];
            dst_ip = splits[3];
            src_port = Integer.parseInt(splits[4]);
            dst_port = Integer.parseInt(splits[5]);
            System.out.println(buf_str);

            for (int i = 0; i < num_rules; i++) {
                buf_str = br.readLine();
                String[] msg_splits = buf_str.split(" ");
                if (msg_splits.length != 4) {
                    System.out.println("ERROR: Expected a rule but received metadata instead");
                    System.out.println("Received: " + buf_str);
                    System.exit(1);
                }
                else if (!msg_id.equals(msg_splits[0])) {
                    System.out.println("ERROR: Non matching msg_ids. Received " + msg_splits[0] + " but expected " + msg_id);
                    System.exit(1);
                }

                dpid = Integer.parseInt(msg_splits[1]);
                out_port = Integer.parseInt(msg_splits[2]);
                forward = msg_splits[3].equals("0");

                System.out.println(buf_str);
                // TODO: Actually set rule
            }

            num_recv++;
        }
        br.close();

        String send_fifo_name = "/tmp/gaia_fifo_to_ctrl";
        f = new File(send_fifo_name);
        FileWriter fw = new FileWriter(f);
        fw.write("1");
        fw.close();
    }
}
