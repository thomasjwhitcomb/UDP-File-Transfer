import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/**
 * UDP file sender with configurable loss and reorder simulation.
 *
 * Usage:
 *   java sender.java [-v] <host> <port> <file> <loss%> <reorder%> <dup%>
 *
 * The sender splits the file into 1000-byte chunks, base64-encodes each,
 * and sends them as UDP datagrams using a stop-and-wait protocol.
 *
 * Loss simulation: on each send attempt, there is a <loss%> chance the
 * packet is silently dropped (not transmitted).  The sender still waits
 * for an ACK, times out, and retransmits — modeling real network loss.
 *
 * Reorder simulation: each outgoing DATA packet has a <reorder%> chance of
 * being buffered.  When the buffer reaches a cap (derived from reorder%)
 * or a packet is not buffered, the buffer is flushed in shuffled order.
 * Cap = max(2, min(6, reorder% / 10)).
 *
 * Duplicate simulation: on each send attempt, there is a <dup%> chance
 * the packet is transmitted twice, causing the receiver to see a duplicate.
 */
public class sender {

    private static final int CHUNK_SIZE = 1000;
    private static final int ACK_TIMEOUT_MS = 2000;
    private static final int MAX_RETRIES = 20;

    private final DatagramSocket socket;
    private final InetAddress destAddr;
    private final int destPort;
    private final boolean verbose;
    private final Random rng = new Random();

    private final String transferId;
    private final String filename;
    private final String checksum;
    private final List<byte[]> chunks;

    private final int lossPct;
    private final int reorderPct;
    private final int dupPct;

    public sender(String host, int port, String filePath,
                  int lossPct, int reorderPct, int dupPct,
                  boolean verbose) throws Exception {
        this.destAddr = InetAddress.getByName(host);
        this.destPort = port;
        this.lossPct = lossPct;
        this.reorderPct = reorderPct;
        this.dupPct = dupPct;
        this.verbose = verbose;

        this.socket = new DatagramSocket();
        this.socket.setSoTimeout(ACK_TIMEOUT_MS);

        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        this.filename = Paths.get(filePath).getFileName().toString();
        this.checksum = md5Hex(fileBytes);
        this.transferId = "xfer-" + UUID.randomUUID().toString().substring(0, 8);

        this.chunks = new ArrayList<>();
        for (int off = 0; off < fileBytes.length; off += CHUNK_SIZE) {
            int end = Math.min(off + CHUNK_SIZE, fileBytes.length);
            this.chunks.add(Arrays.copyOfRange(fileBytes, off, end));
        }
    }

    public void run() throws Exception {
        log("Transfer %s: %s (%d chunks, md5=%s)",
            transferId, filename, chunks.size(), checksum);
        log("Loss=%d%%, Reorder=%d%%, Dup=%d%%", lossPct, reorderPct, dupPct);

        sendBegin();
        sendAllChunks();
        sendEnd();

        log("Transfer %s complete.", transferId);
        socket.close();
    }

    // --- Protocol phases ---

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

            // Reorder simulation: buffer this packet
            if (rng.nextInt(100) < reorderPct && seq < chunks.size() - 1) {
                vlog("  [reorder] buffering seq=%d (%d/%d)",
                    seq, reorderBuf.size() + 1, reorderCap);
                reorderBuf.add(msg);
                reorderSeqs.add(seq);

                // Flush when buffer hits cap
                if (reorderBuf.size() >= reorderCap) {
                    flushReorderBuffer(reorderBuf, reorderSeqs);
                }
                continue;
            }

            // Not buffered — flush any pending buffer first, then send
            if (!reorderBuf.isEmpty()) {
                flushReorderBuffer(reorderBuf, reorderSeqs);
            }
            sendAndWaitAck(msg, String.valueOf(seq));
        }

        // Flush any remaining buffered packets
        if (!reorderBuf.isEmpty()) {
            flushReorderBuffer(reorderBuf, reorderSeqs);
        }
    }

    private void flushReorderBuffer(List<String> buf, List<Integer> seqs)
            throws Exception {
        // Shuffle both lists in the same order
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < buf.size(); i++) indices.add(i);
        Collections.shuffle(indices, rng);

        vlog("  [reorder] flushing %d buffered packets", buf.size());
        for (int i : indices) {
            sendAndWaitAck(buf.get(i), String.valueOf(seqs.get(i)));
        }
        buf.clear();
        seqs.clear();
    }

    private void sendEnd() throws Exception {
        sendAndWaitAck(String.format("END %s", transferId), "END");
    }

    // --- Stop-and-wait with retransmission ---

    private void sendAndWaitAck(String message, String expectedAckTag)
            throws Exception {
        byte[] msgBytes = message.getBytes();
        DatagramPacket outPkt = new DatagramPacket(
            msgBytes, msgBytes.length, destAddr, destPort);

        String expectedReply = String.format("ACK %s %s", transferId, expectedAckTag);
        byte[] buf = new byte[1024];

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {

            // Loss simulation: don't transmit, fall through to ACK wait
            // (will timeout, triggering a retransmit on the next attempt)
            if (rng.nextInt(100) < lossPct) {
                vlog("  [loss] dropped %s (attempt %d)", expectedAckTag, attempt);
            } else {
                socket.send(outPkt);
                vlog("  sent %s (attempt %d)", expectedAckTag, attempt);

                // Dup simulation: transmit a second copy immediately
                if (rng.nextInt(100) < dupPct) {
                    socket.send(outPkt);
                    vlog("  [dup] re-sent %s (attempt %d)", expectedAckTag, attempt);
                }
            }

            // Read replies until we get the expected ACK or timeout.
            // Stale ACKs (from dup sends) are discarded without retransmitting.
            try {
                while (true) {
                    DatagramPacket inPkt = new DatagramPacket(buf, buf.length);
                    socket.receive(inPkt);
                    String reply = new String(
                        inPkt.getData(), 0, inPkt.getLength()).trim();

                    if (reply.equals(expectedReply)) {
                        vlog("  got %s", reply);
                        return;
                    }

                    vlog("  stale reply: %s (wanted: %s)", reply, expectedReply);
                }
            } catch (SocketTimeoutException e) {
                vlog("  timeout waiting for %s", expectedReply);
            }
        }

        throw new IOException(String.format(
            "gave up after %d retries for %s %s",
            MAX_RETRIES, transferId, expectedAckTag));
    }

    // --- Helpers ---

    private static String md5Hex(byte[] data) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private void log(String fmt, Object... args) {
        System.err.printf("[sender] " + fmt + "%n", args);
    }

    private void vlog(String fmt, Object... args) {
        if (verbose) System.err.printf("[sender] " + fmt + "%n", args);
    }

    // --- Entry point ---

    public static void main(String[] args) throws Exception {
        int ai = 0;
        boolean verbose = false;
        if (args.length > 0 && args[0].equals("-v")) {
            verbose = true;
            ai = 1;
        }

        if (args.length - ai != 6) {
            System.err.println(
                "Usage: java sender.java [-v] <host> <port> <file> <loss%> <reorder%> <dup%>");
            System.exit(1);
        }

        String host = args[ai];
        int port = Integer.parseInt(args[ai + 1]);
        String filePath = args[ai + 2];
        int lossPct = Integer.parseInt(args[ai + 3]);
        int reorderPct = Integer.parseInt(args[ai + 4]);
        int dupPct = Integer.parseInt(args[ai + 5]);

        sender s = new sender(host, port, filePath, lossPct, reorderPct, dupPct, verbose);
        s.run();
    }
}
