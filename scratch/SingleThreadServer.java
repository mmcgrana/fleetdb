import java.io.*;
import java.net.*;

public class SingleThreadServer {
  public static void main(String[] args) throws IOException {
    ServerSocket serverSocket = new ServerSocket(4444);
    while (true) {
      Socket socket = (serverSocket.accept());
      try {
        PrintWriter out = new PrintWriter(socket.getOutputStream());
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String msg = in.readLine();
        if (msg.equals("ping")) {
          out.println("pong");
          out.close();
          in.close();
          socket.close();
        } else {
          System.err.print("Unexpected req: ");
          System.err.println(msg);
          System.err.flush();
          System.exit(1);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
