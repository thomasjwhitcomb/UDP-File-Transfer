import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Automated test harness: sends multiple files concurrently to a receiver
 * and reports PASS/FAIL per transfer.
 *
 * Usage:
 *   java tsender.java <host> <port> <loss%> <reorder%> <dup%> <file1> [file2 ...]
 *
 * Use 'auto' as a filename to generate small/medium/large test files.
 */
public class tsender {

    private static final int CHUNK_SIZE = 1000;
    private static final int ACK_TIMEOUT_MS = 2000;
    private static final int MAX_RETRIES = 20;

    private final String host;
    private final int port;
    private final int lossPct;
    private final int reorderPct;
    private final int dupPct;
    private final List<String> filePaths;

    public tsender(String host, int port, int lossPct, int reorderPct,
                   int dupPct, List<String> filePaths) {
        this.host = host;
        this.port = port;
        this.lossPct = lossPct;
        this.reorderPct = reorderPct;
        this.dupPct = dupPct;
        this.filePaths = filePaths;
    }

    // --- One transfer (self-contained sender logic) ---

    private class Transfer implements Callable<Result> {
        private final String filePath;
        private final DatagramSocket socket;
        private final InetAddress destAddr;
        private final Random rng = new Random();

        private String transferId;
        private String filename;
        private String checksum;
        private List<byte[]> chunks;

        Transfer(String filePath) throws Exception {
            this.filePath = filePath;
            this.socket = new DatagramSocket();
            this.socket.setSoTimeout(ACK_TIMEOUT_MS);
            this.destAddr = InetAddress.getByName(host);
        }

        @Override
        public Result call() {
            try {
                loadFile();
                sendBegin();
                sendAllChunks();
                sendEnd();
                socket.close();
                return new Result(true, filename, null);
            } catch (Exception e) {
                socket.close();
                return new Result(false,
                    Paths.get(filePath).getFileName().toString(),
                    e.getMessage());
            }
        }

        private void loadFile() throws Exception {
            byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
            this.filename = Paths.get(filePath).getFileName().toString();
            this.checksum = md5Hex(fileBytes);
            this.transferId = "test-" + UUID.randomUUID().toString().substring(0, 8);

            this.chunks = new ArrayList<>();
            for (int off = 0; off < fileBytes.length; off += CHUNK_SIZE) {
                int end = Math.min(off + CHUNK_SIZE, fileBytes.length);
                this.chunks.add(Arrays.copyOfRange(fileBytes, off, end));
            }
        }

        private void sendBegin() throws Exception {
            String msg = String.format("BEGIN %s %s %d %s",
                transferId, filename, chunks.size(), checksum);
            sendAndWaitAck(msg, "BEGIN");
        }

        private void sendAllChunks() throws Exception {
            int reorderCap = Math.max(2, Math.min(6, reorderPct / 10));
            List<String> reorderBuf = new ArrayList<>();
            List<Integer> reorderSeqs = new ArrayList<>();

            for (int seq = 0; seq < chunks.size(); seq++) {
                String b64 = Base64.getEncoder().encodeToString(chunks.get(seq));
                String msg = String.format("DATA %s %d %s", transferId, seq, b64);

                if (rng.nextInt(100) < reorderPct && seq < chunks.size() - 1) {
                    reorderBuf.add(msg);
                    reorderSeqs.add(seq);
                    if (reorderBuf.size() >= reorderCap) {
                        flushReorderBuffer(reorderBuf, reorderSeqs);
                    }
                    continue;
                }

                if (!reorderBuf.isEmpty()) {
                    flushReorderBuffer(reorderBuf, reorderSeqs);
                }
                sendAndWaitAck(msg, String.valueOf(seq));
            }

            if (!reorderBuf.isEmpty()) {
                flushReorderBuffer(reorderBuf, reorderSeqs);
            }
        }

        private void flushReorderBuffer(List<String> buf, List<Integer> seqs)
                throws Exception {
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < buf.size(); i++) indices.add(i);
            Collections.shuffle(indices, rng);
            for (int i : indices) {
                sendAndWaitAck(buf.get(i), String.valueOf(seqs.get(i)));
            }
            buf.clear();
            seqs.clear();
        }

        private void sendEnd() throws Exception {
            sendAndWaitAck(String.format("END %s", transferId), "END");
        }

        private void sendAndWaitAck(String message, String expectedAckTag)
                throws Exception {
            byte[] msgBytes = message.getBytes();
            DatagramPacket outPkt = new DatagramPacket(
                msgBytes, msgBytes.length, destAddr, port);
            String expectedReply = String.format(
                "ACK %s %s", transferId, expectedAckTag);
            byte[] buf = new byte[1024];

            for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {

                // Loss simulation: skip transmit, fall through to ACK wait
                if (rng.nextInt(100) < lossPct) {
                    // don't send — will timeout and retry
                } else {
                    socket.send(outPkt);

                    // Dup simulation: transmit a second copy immediately
                    if (rng.nextInt(100) < dupPct) {
                        socket.send(outPkt);
                    }
                }

                // Read replies until expected ACK or timeout.
                // Stale ACKs (from dup sends) are discarded.
                try {
                    while (true) {
                        DatagramPacket inPkt = new DatagramPacket(buf, buf.length);
                        socket.receive(inPkt);
                        String reply = new String(
                            inPkt.getData(), 0, inPkt.getLength()).trim();
                        if (reply.equals(expectedReply)) return;
                    }
                } catch (SocketTimeoutException e) {
                    // retry
                }
            }
            throw new IOException("gave up after " + MAX_RETRIES +
                " retries for " + transferId + " " + expectedAckTag);
        }
    }

    // --- Orchestration ---

    public int run() throws Exception {
        System.err.printf("[tsender] %d transfers, loss=%d%%, reorder=%d%%, dup=%d%%%n",
            filePaths.size(), lossPct, reorderPct, dupPct);

        ExecutorService pool = Executors.newFixedThreadPool(filePaths.size());
        List<Future<Result>> futures = new ArrayList<>();

        for (String fp : filePaths) {
            futures.add(pool.submit(new Transfer(fp)));
        }

        pool.shutdown();

        int passed = 0;
        int failed = 0;

        for (Future<Result> f : futures) {
            Result r = f.get();
            if (r.success) {
                System.err.printf("[tsender] PASS %s%n", r.filename);
                passed++;
            } else {
                System.err.printf("[tsender] FAIL %s: %s%n", r.filename, r.error);
                failed++;
            }
        }

        System.err.printf("[tsender] Results: %d passed, %d failed%n", passed, failed);
        return failed;
    }

    // --- Helpers ---

    private static String md5Hex(byte[] data) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private static List<String> generateTestFiles() throws IOException {
        List<String> paths = new ArrayList<>();
        paths.add(createTestFile("testfile_auto_small.txt", 500));
        paths.add(createTestFile("testfile_auto_medium.txt", 4500));
        paths.add(createTestFile("testfile_auto_large.txt", 19500));
        return paths;
    }

    private static String createTestFile(String name, int size) throws IOException {
        Random rng = new Random(42);
        byte[] data = new byte[size];
        rng.nextBytes(data);
        Files.write(Paths.get(name), data);
        return name;
    }

    private static void cleanupAutoFiles(List<String> filePaths) {
        for (String fp : filePaths) {
            if (fp.startsWith("testfile_auto_")) {
                try { Files.deleteIfExists(Paths.get(fp)); } catch (IOException e) {}
            }
        }
    }

    // --- Result ---

    private static class Result {
        final boolean success;
        final String filename;
        final String error;

        Result(boolean success, String filename, String error) {
            this.success = success;
            this.filename = filename;
            this.error = error;
        }
    }

    // --- Entry point ---

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println(
                "Usage: java tsender.java <host> <port> <loss%> <reorder%> <dup%> <file1> [file2 ...]");
            System.err.println(
                "       Use 'auto' as file to generate test files automatically.");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int lossPct = Integer.parseInt(args[2]);
        int reorderPct = Integer.parseInt(args[3]);
        int dupPct = Integer.parseInt(args[4]);

        List<String> filePaths = new ArrayList<>();
        for (int i = 5; i < args.length; i++) {
            if (args[i].equals("auto")) {
                filePaths.addAll(generateTestFiles());
            } else {
                filePaths.add(args[i]);
            }
        }

        tsender t = new tsender(host, port, lossPct, reorderPct, dupPct, filePaths);
        int failed = t.run();

        cleanupAutoFiles(filePaths);
        System.exit(failed > 0 ? 1 : 0);
    }
}
