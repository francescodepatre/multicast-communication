import java.io.*;
import java.net.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class Server {

    private final int port;
    private final int numNodes;
    private final List<ClientHandler> clients = new CopyOnWriteArrayList<>();
    private final Set<Integer> registered = ConcurrentHashMap.newKeySet();
    private final Set<Integer> completed = ConcurrentHashMap.newKeySet();
    private final CountDownLatch allRegistered;
    private final CountDownLatch allCompleted;

    public Server(int port, int numNodes) {
        this.port = port;
        this.numNodes = numNodes;
        this.allRegistered = new CountDownLatch(numNodes);
        this.allCompleted = new CountDownLatch(numNodes);
    }

    public void start() throws IOException, InterruptedException {
        ServerSocket serverSocket = new ServerSocket(port);
        log("ascolto su " + port + ".\nattesa registrazione " + numNodes + " nodi");

        Thread acceptThread = new Thread(() -> {
            while (clients.size() < numNodes) {
                try {
                    Socket s = serverSocket.accept();
                    ClientHandler handler = new ClientHandler(s);
                    clients.add(handler);
                    handler.start();
                } catch (IOException e) {
                    break;
                }
            }
        });
        acceptThread.setDaemon(true);
        acceptThread.start();

        allRegistered.await();
        log("tutti i  nodi si sono registrati. inizio");
        broadcast("start");

        allCompleted.await();
        log("tutti i nodi hanno completato lo scambio di messaggi, terminazione");
        broadcast("stop");

        Thread.sleep(500);
        for (ClientHandler c : clients) c.close();
        serverSocket.close();
        log("sistema terminato.");
    }

    private void broadcast(String msg) {
        for (ClientHandler c : clients) c.send(msg);
    }

    private void log(String msg) {
        System.out.println("server " + msg);
    }

    class ClientHandler extends Thread {
        private final Socket socket;
        private final PrintWriter out;
        private final BufferedReader in;

        ClientHandler(Socket socket) throws IOException {
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }

        void send(String msg) {
            out.println(msg);
        }

        void close() {
            try { socket.close(); } catch (IOException ignored) {}
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.startsWith("reg:")) {
                        int id = Integer.parseInt(line.substring("reg:".length()));
                        if (registered.add(id)) {
                            log("nodo: " + id + " registrato (" + registered.size() + "/" + numNodes + ")");
                            allRegistered.countDown();
                            log("nodo: " + id + " registrato");
                            allRegistered.countDown();
                            log("latch: " + allRegistered.getCount());
                        }
                    } else if (line.startsWith("terminato:")) {
                        int id = Integer.parseInt(line.substring("terminato:".length()));
                        if (completed.add(id)) {
                            log("nodo " + id + " ha completato (" + completed.size() + "/" + numNodes + ")");
                            allCompleted.countDown();
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 6000;
        int numNodes = 15;
        new Server(port, numNodes).start();
    }
}