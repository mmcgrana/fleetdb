import java.io.*;
import java.net.*;

public class ThreadPerConnPersistentServer {
  
  private static class ServerHandler extends Thread {
    private final Socket socket;
    public ServerHandler(Socket socket) { this.socket = socket; }
    
    public void run() {
      try {
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
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  public static void main(String[] args) throws IOException {
    ServerSocket serverSocket = new ServerSocket(4444);
    while (true) {
      new ServerHandler(serverSocket.accept()).start();
    }
  }
}
