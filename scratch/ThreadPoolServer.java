import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class ThreadPoolServer {
  
  private static class ServerHandler implements Runnable {
    private final Socket socket;
    public ServerHandler(Socket socket) { this.socket = socket; }
    
    public void run() {
      try {
        PrintWriter out = new PrintWriter(socket.getOutputStream());
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        in.readLine();
        // if (in.readLine().equals("pin");
        out.println("HTTP/1.0 200 OK");
        out.println();
        out.println("Content-Length: 1354");
        out.println("<html>hi</html>");
        out.close();
        in.close();
        socket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  public static void main(String[] args) throws IOException {
    ServerSocket serverSocket = new ServerSocket(4444, 10000);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    
    while (true) {
      executor.execute(new ServerHandler(serverSocket.accept()));
    }
  }
}
