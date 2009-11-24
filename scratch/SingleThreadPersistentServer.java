import java.io.*;
import java.net.*;

public class SingleThreadPersistentServer {
  public static void main(String[] args) throws IOException {
    ServerSocket serverSocket = new ServerSocket(4444);
    Socket socket = (serverSocket.accept());
    PrintWriter out = new PrintWriter(socket.getOutputStream());
    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    while (true) {
      String msg = in.readLine();
      if (msg.equals("ping")) {
        out.println("pong");
        out.flush();
      } else {
        System.err.print("Unexpected request: ");
        System.err.println(msg);
        System.err.flush();
        System.exit(1);
      }
    }
  }
}
