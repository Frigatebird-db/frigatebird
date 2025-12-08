use glommio::prelude::*;
use glommio::net::TcpListener;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use flume;
use std::thread;

fn main() {
    // work queue - mpmc
    let (work_tx, work_rx) = flume::bounded::<(Vec<u8>, flume::Sender<Vec<u8>>)>(1024);

    // spawn a couple workers
    for i in 0..2 {
        let rx = work_rx.clone();
        thread::spawn(move || {
            while let Ok((data, resp_tx)) = rx.recv() {
                // simulate work
                thread::sleep(std::time::Duration::from_millis(100));
                let response = format!("worker {} processed: {:?}", i, data).into_bytes();
                let _ = resp_tx.send(response);
            }
        });
    }

    // network thread
    LocalExecutorBuilder::default()
        .spawn(|| async move {
            let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
            println!("listening on 8080");

            loop {
                let stream = listener.accept().await.unwrap();
                let work_tx = work_tx.clone();

                glommio::spawn_local(async move {
                    let mut buf = [0u8; 1024];
                    let mut stream = stream;

                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                let (resp_tx, resp_rx) = flume::bounded(1);
                                work_tx.send((buf[..n].to_vec(), resp_tx)).unwrap();

                                // this should yield, not block
                                let response = resp_rx.recv_async().await.unwrap();
                                if stream.write_all(&response).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                })
                .detach();
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
