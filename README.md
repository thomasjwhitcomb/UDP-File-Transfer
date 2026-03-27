# UDP File Transfer
#### Implemented a reliable UDP file transfer protocol receiver in Java that reassembles out-of-order chunks, handles duplicate packets with idempotent ACKs, verifies file integrity via MD5 checksum, and manages concurrent transfers using thread-safe data structures with automatic timeout cleanup.

NOTE: tsender.java and sender.java are both provided course work clients and I DO NOT CLAIM PRODUCTION CREDIT FOR EITHER. Each are provided solely for testing against the receiver.java file.
