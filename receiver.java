import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Reliable UDP file transfer receiver.
 *
 * Usage:
 *   java receiver.java [-v] <port>
 *
 * This receiver listens for UDP datagrams from the sender, reassembles
 * file chunks in order, and verifies the result via MD5 checksum.
 *
 * You must implement the methods marked TODO below.
 */
public class receiver {

    // Active transfers, keyed by transfer ID.
    private final ConcurrentHashMap<String, Transfer> transfers = new ConcurrentHashMap<>();

    // IDs of completed transfers.  Used to re-ACK late DATA/END messages
    // after the transfer has been removed from the active map.
    private final Set<String> completed = ConcurrentHashMap.newKeySet();

    // Lock this object around every System.out call so that output lines
    // from different threads never interleave.
    private final Object outputLock = new Object();

    private final DatagramSocket socket;
    private final boolean verbose;

    private static final long TIMEOUT_MS = 30_000;

    // ----------------------------------------------------------------
    //  Transfer — tracks the state of one file being received.
    //
    //  Provided.  Do not modify.
    //
    //  Each method is synchronized so that the cleanup thread and the
    //  main receive loop can safely access the same Transfer object.
    // ----------------------------------------------------------------

    private class Transfer {
        final String transferId;
        final String filename;
        final int totalChunks;
        final String expectedChecksum;
        final byte[][] chunks;
        final boolean[] received;
        int receivedCount;
        long lastActivity;

        Transfer(String transferId, String filename,
                 int totalChunks, String checksum) {
            this.transferId = transferId;
            this.filename = filename;
            this.totalChunks = totalChunks;
            this.expectedChecksum = checksum;
            this.chunks = new byte[totalChunks][];
            this.received = new boolean[totalChunks];
            this.receivedCount = 0;
            this.lastActivity = System.currentTimeMillis();
        }

        /** Store a chunk.  Returns false if seq is out of range or a dup. */
        synchronized boolean storeChunk(int seq, byte[] data) {
            lastActivity = System.currentTimeMillis();
            if (seq < 0 || seq >= totalChunks) return false;
            if (received[seq]) return false;
            chunks[seq] = data;
            received[seq] = true;
            receivedCount++;
            return true;
        }

        /** True if this seqnum has already been stored. */
        synchronized boolean isDuplicate(int seq) {
            if (seq < 0 || seq >= totalChunks) return false;
            return received[seq];
        }

        /** True if every chunk has been received. */
        synchronized boolean isComplete() {
            return receivedCount == totalChunks;
        }

        /** Update the last-activity timestamp (e.g., on END). */
        synchronized void touch() {
            lastActivity = System.currentTimeMillis();
        }

        /** True if no packet has arrived for more than TIMEOUT_MS. */
        synchronized boolean isStale(long now) {
            return now - lastActivity > TIMEOUT_MS;
        }

        /**
         * Concatenate all chunks in order and compute the MD5 hex digest.
         * Compare the result to expectedChecksum yourself after calling this.
         */
        synchronized String reassembleAndVerify() throws Exception {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 0; i < totalChunks; i++) {
                bos.write(chunks[i]);
            }
            byte[] fileBytes = bos.toByteArray();
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(fileBytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) hex.append(String.format("%02x", b));
            return hex.toString();
        }
    }

    // ----------------------------------------------------------------
    //  Constructor — provided, do not modify.
    // ----------------------------------------------------------------

    public receiver(int port, boolean verbose) throws SocketException {
        this.socket = new DatagramSocket(port);
        this.verbose = verbose;
    }

    // ----------------------------------------------------------------
    //  run — main receive loop
    //
    //  TODO: Implement this method.
    //
    //  1. Print a startup message to stderr.
    //  2. Call startCleanupThread() to begin timeout scanning.
    //  3. Allocate a byte[] buffer (65535 bytes is the UDP maximum).
    //  4. Loop forever:
    //     a. Create a DatagramPacket wrapping your buffer.
    //     b. Call socket.receive(pkt) to wait for the next datagram.
    //     c. Extract the message string, sender address, and sender port
    //        from the packet.
    //     d. Split the message into tokens (hint: split(" ", 4) keeps
    //        the base64 payload intact as one token).
    //     e. Look at the first token to decide which handler to call:
    //        - "BEGIN" -> handleBegin(addr, port, tokens)
    //        - "DATA"  -> handleData(addr, port, tokens)
    //        - "END"   -> handleEnd(addr, port, tokens)
    //        - anything else -> sendReply(addr, port, "ERR malformed")
    //     f. If a handler throws an exception, send "ERR malformed".
    // ----------------------------------------------------------------

    public void run() throws IOException
    {
        //Print Startup Message
        System.err.println("[receiver] listening on port" + socket.getLocalPort());

        //Call startCleanupThread() to begin timeout scanning
        startCleanupThread();

        //Allocate Buffer with maximum UDP Size
        byte[] buffer = new byte[65535];

        while(true){
            //Create DatagramPacket to wrap the buffer
            DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);

            try {
                //Block until a new datagram arrives
                socket.receive(pkt);

                //Extract the message string from the packet bytes
                String message = new String(pkt.getData(),0, pkt.getLength()).trim();

                // Extract the sender and port to send response
                InetAddress addr = pkt.getAddress();
                int senderPort = pkt.getPort();

                // Split into at most 4 tokens - keeping the base64 payload
                // in tokens[3] as one intact string
                String[] tokens = message.split(" ", 4);

                //Dispatch based on message type
                switch (tokens[0]){
                    case "BEGIN" -> handleBegin(addr, senderPort, tokens);
                    case "DATA"  -> handleData(addr, senderPort, tokens);
                    case "END"   -> handleEnd(addr, senderPort, tokens);
                    default      -> sendReply(addr, senderPort, "ERR malformed");
                }
            } catch (Exception e) {
                // A bad packet cannot crash the receive loop.
                // We don't have an addr/port if receive() itself threw,
                // so we log the exception and continue
                vlog("Exception in receive loop: %s", e.getMessage());
            }
        }
    }

    // ----------------------------------------------------------------
    //  handleBegin — process a BEGIN message
    //
    //  TODO: Implement this method.
    //
    //  The tokens array comes from splitting the datagram with limit 4,
    //  so you may need to re-split to get all 5 fields:
    //    BEGIN <transfer_id> <filename> <total_chunks> <checksum>
    //
    //  Steps:
    //  1. Parse the 5 fields.  If anything is missing or total_chunks
    //     is not a valid integer, sendReply "ERR malformed" and return.
    //  2. If this transfer_id is already active, sendReply
    //     "ERR duplicate transfer" and return.
    //  3. Create a new Transfer and add it to the transfers map.
    //  4. Log: "RECV <transfer_id> BEGIN chunks=<n>"
    //  5. sendReply: "ACK <transfer_id> BEGIN"
    // ----------------------------------------------------------------

    private void handleBegin(InetAddress addr, int port, String[] tokens)
            throws IOException
    {
        // TODO
        // Expected format: BEGIN <transfer_id> <filename> <total_chunks> <checksum>
        // split(" ", 4) gives us:
        //    tokens[0] = "BEGIN"
        //    tokens[1] = transfer_id
        //    tokens[2] = filename
        //    tokens[3] = "<total_chunks> <checksum>"  (two fields still joined)

        if (tokens.length < 4) {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        // Re-split the last token to separate total_chunks from checksum
        String[] rest = tokens[3].split(" ");
        if (rest.length < 2) {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        String transferId = tokens[1];
        String filename   = tokens[2];
        String checksum   = rest[1];

        // Parse total_chunks as an integer
        int totalChunks;
        try {
            totalChunks = Integer.parseInt(rest[0]);
        } catch (NumberFormatException e) {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        // Reject if a transfer with this ID is already active.
        if (transfers.containsKey(transferId)) {
            sendReply(addr, port, "ERR duplicate transfer");
            return;
        }

        // Register the new transfer
        Transfer t = new Transfer(transferId, filename, totalChunks, checksum);
        transfers.put(transferId, t);

        // Required log
        log("RECV %s BEGIN chunks=%d", transferId, totalChunks);

        // ACK so the senders proceeds to send DATA
        sendReply(addr, port, "ACK " + transferId + " BEGIN");
    }

    // ----------------------------------------------------------------
    //  handleData — process a DATA message
    //
    //  TODO: Implement this method.
    //
    //  Tokens: DATA <transfer_id> <seqnum> <base64_payload>
    //
    //  Steps:
    //  1. Validate token count (must be 4).
    //  2. Look up the transfer by ID.
    //     - If not found but the ID is in the completed set, re-ACK
    //       the seqnum so the sender stops retrying, and return.
    //     - If not found at all, sendReply "ERR unknown transfer".
    //  3. Parse seqnum.  Validate range (0 to totalChunks-1).
    //  4. Check for duplicate (isDuplicate).  If so:
    //     - Log: "DUP <transfer_id> seq=<seqnum>"
    //     - Re-send the ACK (the sender may not have received it).
    //     - Return.
    //  5. Decode the base64 payload (Base64.getDecoder().decode()).
    //  6. Store the chunk (storeChunk).
    //  7. Log: "RECV <transfer_id> DATA seq=<seqnum>"
    //  8. sendReply: "ACK <transfer_id> <seqnum>"
    //  9. If the transfer is now complete, call completeTransfer(t).
    // ----------------------------------------------------------------

    private void handleData(InetAddress addr, int port, String[] tokens)
            throws Exception
    {
        if (tokens.length < 4) {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        String transferId = tokens[1];

        // Look up the active transfer
        Transfer t = transfers.get(transferId);

        if (t == null){
            // Transfer not active, check if already completed
             if (completed.contains(transferId)) {
                 // Re-ACK this seq num so the sender stops retrying
                 sendReply(addr, port, "ACK " + transferId + " " + tokens[2]);
             } else {
                 sendReply(addr, port, "ERR Unknown transfer");
             }
             return;
        }

        // Parse and validate the sequence number
        int seq;
        try {
            seq = Integer.parseInt(tokens[2]);
        } catch (NumberFormatException e) {
            sendReply(addr, port, "ERR malformed");
            return;
        }


        if (seq < 0 || seq >= t.totalChunks) {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        // Duplicate detection - this chunk was already stored successfully
        if (t.isDuplicate(seq)){
            // Log the duplicate
            log("DUP %s seq=%d", transferId, seq);

            // Re-ACK the duplicate since the sender never got the original ACK
            sendReply(addr, port, "ACK " + transferId + " " + seq);
            return;
        }

        // Decode the base64 payload back to raw bytes
        byte[] chunkBytes = Base64.getDecoder().decode(tokens[3]);

        // Store in the transfer's chunk array
        t.storeChunk(seq, chunkBytes);

        // Required stdout log line for a new chunk
        log("RECV %s DATA seq=%d", transferId, seq);

        // ACK this chunk so the sender moves to the next chunk
        sendReply(addr, port, "ACK " + transferId + " " + seq);

        // Check if this was the last missing chunk.
        // This handles the case where END arrived before all DATA,
        // and this DATA packet is the one that completes the transfer.
        if (t.isComplete()){
            completeTransfer(t);
        }
    }

    // ----------------------------------------------------------------
    //  handleEnd — process an END message
    //
    //  TODO: Implement this method.
    //
    //  Tokens: END <transfer_id>
    //
    //  Steps:
    //  1. Validate token count.
    //  2. Look up the transfer by ID.
    //     - If not found but completed, re-ACK END and return.
    //     - If not found at all, sendReply "ERR unknown transfer".
    //  3. Call t.touch() to update the activity timestamp.
    //  4. sendReply: "ACK <transfer_id> END"
    //  5. If the transfer is complete, call completeTransfer(t).
    // ----------------------------------------------------------------

    private void handleEnd(InetAddress addr, int port, String[] tokens)
            throws IOException
    {
        if (tokens.length < 2){
            sendReply(addr, port, "ERR malformed");
            return;
        }

        String transferId = tokens[1];

        // Look up the active transfer
        Transfer t = transfers.get(transferId);

        if (t == null) {
            // Not active - check if completed already
            if (completed.contains(transferId)){
                // Re-ACK END so the sender stops retrying
                sendReply(addr, port, "ACK " + transferId + " END");
            } else {
                sendReply(addr, port, "ERR unknown transfer");
            }
            return;
        }

        // Reset the inactivity timer - END may arrive before all
        // DATA packets (due to reordering), and we don't want the
        // cleanup thread to discard a still active transfer
        t.touch();

        // ACK END unconditionally - the sender needs this to proceed
        sendReply(addr, port, "ACK " + transferId + " END");

        // If all chunks are already in, finalize the transfer
        if (t.isComplete()){
            completeTransfer(t);
        }
        // If not complete, do nothing. The sender will retransmit missing
        // DATA chunks, and the last handleData() call will trigger completeTransfer()
    }

    // ----------------------------------------------------------------
    //  completeTransfer — reassemble the file and verify the checksum
    //
    //  TODO: Implement this method.
    //
    //  Steps:
    //  1. Call t.reassembleAndVerify() to get the computed MD5 hex.
    //  2. Compare it to t.expectedChecksum.
    //  3. Log: "COMPLETE <transfer_id> file=<filename> status=OK"
    //     or   "COMPLETE <transfer_id> file=<filename> status=FAIL"
    //  4. Add the transfer ID to the completed set.
    //  5. Remove the transfer from the transfers map.
    //
    //  If reassembleAndVerify throws, log status=FAIL.
    // ----------------------------------------------------------------

    private void completeTransfer(Transfer t) {
        //Assume failure unless confirmed otherwise
        String status = "FAIL";

        try
        {
            //Call t.reassembleAndVerify() to get the computed MD5 Hex
            String computedCheckSum = t.reassembleAndVerify();

            if (computedCheckSum.equals(t.expectedChecksum)){
                status = "OK";
            }
        } catch (Exception e) {
            // If reassembly throws an exception, status stays fail
            vlog("Reassembly error for %s: %s", t.transferId, e.getMessage());
        }

        // Print the required COMPLETE line to stdout
        log("COMPLETE %s file=%s status=%s", t.transferId, t.filename, status);

        // Add to completed set before removing from active map.
        // This ensures that any in-flight DATA/END packets that arrive
        // after removal get re-ACKed instead of receiving ERR unknown transfer,
        // which would cause the sender to exhaust its retries and fail
        completed.add(t.transferId);
        transfers.remove(t.transferId);
    }

    // ----------------------------------------------------------------
    //  Cleanup thread — provided, do not modify.
    //
    //  Runs as a daemon thread.  Every 5 seconds, scans the transfer
    //  registry and removes any transfer that has been idle for more
    //  than TIMEOUT_MS.  Logs a TIMEOUT line for each removed transfer.
    // ----------------------------------------------------------------

    private void startCleanupThread() {
        Thread cleaner = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    long now = System.currentTimeMillis();
                    for (Map.Entry<String, Transfer> e : transfers.entrySet()) {
                        if (e.getValue().isStale(now)) {
                            transfers.remove(e.getKey());
                            log("TIMEOUT %s", e.getKey());
                        }
                    }
                } catch (InterruptedException ex) {
                    break;
                }
            }
        });
        cleaner.setDaemon(true);
        cleaner.start();
    }

    // ----------------------------------------------------------------
    //  I/O helpers — provided, do not modify.
    // ----------------------------------------------------------------

    /** Send a UDP reply datagram to the given address and port. */
    private void sendReply(InetAddress addr, int port, String message)
            throws IOException {
        byte[] data = message.getBytes();
        DatagramPacket pkt = new DatagramPacket(data, data.length, addr, port);
        socket.send(pkt);
        vlog("sent to %s:%d: %s", addr, port, message);
    }

    /** Print a line to stdout (thread-safe). */
    private void log(String fmt, Object... args) {
        synchronized (outputLock) {
            System.out.printf(fmt + "%n", args);
            System.out.flush();
        }
    }

    /** Print a line to stderr if verbose mode is on. */
    private void vlog(String fmt, Object... args) {
        if (verbose) System.err.printf("[receiver] " + fmt + "%n", args);
    }

    // ----------------------------------------------------------------
    //  Entry point — provided, do not modify.
    // ----------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        int ai = 0;
        boolean verbose = false;
        if (args.length > 0 && args[0].equals("-v")) {
            verbose = true;
            ai = 1;
        }

        if (args.length - ai != 1) {
            System.err.println("Usage: java receiver.java [-v] <port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[ai]);
        receiver r = new receiver(port, verbose);
        r.run();
    }
}
