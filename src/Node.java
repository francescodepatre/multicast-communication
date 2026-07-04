import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Node {

    private final int id;
    private final double lp;
    private final String serverHost;
    private final int serverPort;
    private final String multicastAddress;
    private final int multicastPort;

    private MulticastSocket mcSocket;
    private InetAddress group;
    private Socket serverSocket;
    private PrintWriter serverOut;
    private BufferedReader serverIn;

    private final Map<Integer, Integer> expectedId = new ConcurrentHashMap<>();

    private final Map<Integer, Message> sentLog = new ConcurrentHashMap<>();

    private final Map<String, Message> pendingLossRequests = new ConcurrentHashMap<>();

    private final Random random = new Random();
    private final ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean running = true;

    public Node(int id, double lp, String serverHost, int serverPort, String multicastAddress, int multicastPort) {
        this.id = id;
        this.lp = lp;
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.multicastAddress = multicastAddress;
        this.multicastPort = multicastPort;
    }

    public void start() throws IOException {
        mcSocket = new MulticastSocket(multicastPort);
        mcSocket.setReuseAddress(true);
        group = InetAddress.getByName(multicastAddress);
        mcSocket.joinGroup(group);

        Thread receiver = new Thread(this::receiveLoop, "Nodo-" + id + "-Receiver");
        receiver.setDaemon(true);
        receiver.start();

        retryExecutor.scheduleAtFixedRate(() -> {
            for (Message notice : pendingLossRequests.values()) {
                multicastSend(notice);
                log("ripete richiesta di ritrasmissione per messaggio id " + notice.msgId + " del nodo " + notice.senderId);
            }
        }, 2, 2, TimeUnit.SECONDS);

        serverSocket = new Socket(serverHost, serverPort);
        serverOut = new PrintWriter(serverSocket.getOutputStream(), true);
        serverIn = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));

        serverOut.println("reg:" + id);
        log("registrato presso il server, in attesa del comando start");

        waitForServerMessage("start");
        log("ricevuto start dal server, inizio l'invio dei messaggi");

        sendLoop();

        serverOut.println("terminato:" + id);
        log("inviati 100 messaggi, invio completamento al server");

        waitForServerMessage("stop");
        log("ricevuto stop dal server, termino l'esecuzione");

        shutdown();
    }

    private void waitForServerMessage(String expected) throws IOException {
        String line;
        while ((line = serverIn.readLine()) != null) {
            if (line.equals(expected)) return;
        }
    }

    private void sendLoop() {
        int msgId = 1;
        while (msgId <= 100 && running) {
            Message m = new Message(Message.Type.DATA, id, msgId);
            sentLog.put(msgId, m);
            multicastSend(m);
            log("inviato data id=" + msgId);
            msgId++;
            sleepRandom();
        }
    }

    private void receiveLoop() {
        byte[] buf = new byte[4096];
        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                mcSocket.receive(packet);
                Message m = Message.deserialize(packet.getData(), packet.getLength());

                if (m.type != Message.Type.LOSS_NOTICE && m.senderId == id) continue;

                if (random.nextDouble() > lp) {
                    log("messaggio perso: " + m);
                    continue;
                }

                handleMessage(m);
            } catch (IOException e) {
                if (running) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void handleMessage(Message m) {
        switch (m.type) {
            case DATA, RESEND -> handleData(m.senderId, m.msgId, m.type == Message.Type.RESEND);
            case LOSS_NOTICE -> handleLossNotice(m.senderId, m.msgId);
        }
    }

    private void handleData(int originId, int msgId, boolean isResend) {
        if (originId == id) return;

        int expected = expectedId.getOrDefault(originId, 1);

        if (msgId == expected) {
            expectedId.put(originId, expected + 1);
            pendingLossRequests.remove(originId + ":" + expected);
            log("ricevuto" + (isResend ? " (ritrasmesso)" : "") + " messaggio da nodo " + originId + " id " + msgId);
        } else if (msgId > expected) {

            String key = originId + ":" + expected;
            log("rilevata perdita: atteso id " + expected + " da nodo " + originId + ", ricevuto invece id" + msgId);
            if (!pendingLossRequests.containsKey(key)) {
                Message notice = new Message(Message.Type.LOSS_NOTICE, originId, expected);
                pendingLossRequests.put(key, notice);
                multicastSend(notice);
            }
        } else {
            log("messaggio duplicato ignorato/superato da nodo " + originId + " id " + msgId);
        }
    }

    private void handleLossNotice(int originId, int lostMsgId) {
        if (originId != id){
            return;
        }

        Message original = sentLog.get(lostMsgId);
        if (original != null) {
            Message resend = new Message(Message.Type.RESEND, id, lostMsgId);
            multicastSend(resend);
            log("ricevuta richiesta di ritrasmissione: rinvio messaggio id " + lostMsgId);
        }
    }

    private void multicastSend(Message m) {
        try {
            byte[] data = m.serialize();
            DatagramPacket packet = new DatagramPacket(data, data.length, group, multicastPort);
            mcSocket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sleepRandom() {
        try {
            Thread.sleep(30 + random.nextInt(70));
        } catch (InterruptedException ignored) {}
    }

    private void shutdown() {
        running = false;
        retryExecutor.shutdownNow();
        try { mcSocket.leaveGroup(group); } catch (IOException ignored) {}
        mcSocket.close();
        try { serverSocket.close(); } catch (IOException ignored) {}
    }

    private void log(String msg) {
        System.out.println("nodo " + id + " " + msg);
    }

    public static void main(String[] args) throws Exception {
        int id = Integer.parseInt(args[0]);
        double lp = Double.parseDouble(args[1]);
        String serverHost = args[2];
        int serverPort = Integer.parseInt(args[3]);
        String mcAddr = args[4];
        int mcPort = args.length > 5 ? Integer.parseInt(args[5]) : 4446;

        new Node(id, lp, serverHost, serverPort, mcAddr, mcPort).start();
    }
}