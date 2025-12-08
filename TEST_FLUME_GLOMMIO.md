# Testing Flume + Glommio Non-Blocking Behavior

## Setup

```bash
# Increase memlock limit (required for glommio)
ulimit -l unlimited

# Or permanently in /etc/security/limits.conf:
# * soft memlock unlimited
# * hard memlock unlimited

# Build and run
cd /tmp/test_tcp_worker_pool
cargo build --release
./target/release/test_tcp_worker_pool
```

## Test Scenario

The server is configured so:
- **First request**: Worker receives it but NEVER responds (simulates blocking forever)
- **Subsequent requests**: Workers respond normally after 100ms

## How to Test

### Terminal 1: Run the server
```bash
cd /tmp/test_tcp_worker_pool
cargo run --release
```

You should see: `listening on 8080`

### Terminal 2: Send first request (will hang)
```bash
echo "request1" | nc localhost 8080
# This will hang forever - worker never responds
```

You should see server output:
```
Connection 1: ACCEPTED
Connection 1: Got 9 bytes, sending to worker...
Worker 0: Got first request, BLOCKING FOREVER (not sending response)
Connection 1: Waiting for response (should YIELD, not block)...
```

### Terminal 3: Send second request (should respond immediately)
```bash
echo "request2" | nc localhost 8080
```

## Expected Result

**✅ PASS**: Connection 2 gets response immediately while Connection 1 still hangs
```
Connection 2: ACCEPTED
Connection 2: Got 9 bytes, sending to worker...
Worker 1: Sending response
Connection 2: Got response, sending back to client
worker 1 processed: [114, 101, 113, 117, 101, 115, 116, 50, 10]
Connection 2: CLOSED
```

**❌ FAIL**: Connection 2 also hangs (would mean recv_async() blocks the executor)

## What This Proves

If Connection 2 responds while Connection 1 is waiting:
→ `flume::recv_async().await` **YIELDS** properly in glommio
→ Pattern is correct for async/blocking bridge

If Connection 2 hangs:
→ `flume::recv_async().await` **BLOCKS** the executor
→ Pattern doesn't work
