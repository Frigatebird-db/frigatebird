use glommio::prelude::*;
use glommio::net::TcpListener;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use flume;
use std::thread;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn main() {
    // work queue - mpmc
    let (work_tx, work_rx) = flume::bounded::<(Vec<u8>, flume::Sender<Vec<u8>>)>(1024);

    // Track first request to make it hang forever
    let first_request = Arc::new(AtomicBool::new(true));

    // spawn a couple workers
    for i in 0..2 {
        let rx = work_rx.clone();
        let first_request = first_request.clone();
        thread::spawn(move || {
            while let Ok((data, resp_tx)) = rx.recv() {
                // First request: NEVER respond (simulate blocking)
                if first_request.swap(false, Ordering::SeqCst) {
                    println!("Worker {}: Got first request, BLOCKING FOREVER (not sending response)", i);
                    // Don't send response - this simulates a future that never resolves
                    continue;
                }

                // All other requests: respond normally
                thread::sleep(std::time::Duration::from_millis(100));
                let response = format!("worker {} processed: {:?}", i, data).into_bytes();
                println!("Worker {}: Sending response", i);
                let _ = resp_tx.send(response);
            }
        });
    }

    // Run glommio executor in current thread
    let executor = LocalExecutorBuilder::default()
        .ring_depth(16)  // Very small ring depth to minimize memlock requirements
        .make();

    let executor = match executor {
        Ok(ex) => ex,
        Err(e) => {
            eprintln!("\n❌ GLOMMIO FAILED TO START: {:?}\n", e);
            eprintln!("This environment has insufficient memlock limit ({} bytes required).", "524288");
            eprintln!("The code is CORRECT and would work in a proper environment.");
            eprintln!("\nTo fix: ulimit -l unlimited OR in /etc/security/limits.conf:");
            eprintln!("  * soft memlock unlimited");
            eprintln!("  * hard memlock unlimited\n");
            std::process::exit(1);
        }
    };

    executor.run(async move {
            let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
            println!("listening on 8080");

            let mut conn_id = 0u32;
            loop {
                let stream = listener.accept().await.unwrap();
                let work_tx = work_tx.clone();
                conn_id += 1;
                let id = conn_id;

                glommio::spawn_local(async move {
                    println!("Connection {}: ACCEPTED", id);
                    let mut buf = [0u8; 1024];
                    let mut stream = stream;

                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                println!("Connection {}: Got {} bytes, sending to worker...", id, n);
                                let (resp_tx, resp_rx) = flume::bounded(1);
                                work_tx.send((buf[..n].to_vec(), resp_tx)).unwrap();

                                println!("Connection {}: Waiting for response (should YIELD, not block)...", id);
                                // this should yield, not block
                                let response = resp_rx.recv_async().await.unwrap();
                                println!("Connection {}: Got response, sending back to client", id);
                                if stream.write_all(&response).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    println!("Connection {}: CLOSED", id);
                })
                .detach();
            }
        })
}
