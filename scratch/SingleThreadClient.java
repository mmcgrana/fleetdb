import java.io.*;
import java.net.*;

public class SingleThreadClient {
  public static void main(String[] args) throws IOException {
    for (int i = 0; i < 100000; i++) {
      if (i % 1000 == 0) {
        System.out.print(".");
        System.out.flush();
      }
      Socket socket = new Socket("localhost", 4444);
      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      out.println("ping");
      String msg = in.readLine();
      if (msg.equals("pong")) {
        in.close();
        out.close();
        socket.close();
      } else {
        System.err.print("Unexpected response: ");
        System.err.println(msg);
        System.err.flush();
        System.exit(1);
      }
    }
    System.out.println();
  }
}