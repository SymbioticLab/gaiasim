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
        while (num_recv < num_needed) {
            buf_str = br.readLine();
            num_recv++;
            System.out.println(buf_str + " have " + num_recv + " / " + num_needed);
        }
        br.close();

        String send_fifo_name = "/tmp/gaia_fifo_to_ctrl";
        f = new File(send_fifo_name);
        FileWriter fw = new FileWriter(f);
        fw.write("1");
        fw.close();
    }
}
