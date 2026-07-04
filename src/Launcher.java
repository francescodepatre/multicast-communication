import java.lang.Exception;

public class Launcher {
    public static void main(String[] args) throws Exception {
        int numNodes = 15;
        double lp = 0.7;

        int serverPort = 6000;
        String mcAddr = "230.0.0.0";
        int mcPort = 4446;

        System.out.println("Avvio sistema con: " + numNodes + " nodi\nLoss prob=" + lp);

        Thread serverThread = new Thread(() -> {
            try {
                new Server(serverPort, numNodes).start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        serverThread.start();

        Thread.sleep(500);

        for (int i = 1; i <= numNodes; i++) {
            final int id = i;
            Thread t = new Thread(() -> {
                try {
                    new Node(id, lp, "localhost", serverPort, mcAddr, mcPort).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            t.start();
            Thread.sleep(100);
        }

        serverThread.join();
        System.out.println("COMUNICAZIONE MULTICAST TERMINATA");
    }
}