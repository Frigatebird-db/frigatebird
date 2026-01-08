use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn synthetic_timestamp(index: usize) -> String {
    let year = 2020 + (index % 5) as i32;
    let month = 1 + ((index / 5) % 12) as i32;
    let day = 1 + ((index / 37) % 28) as i32;
    let hour = ((index / 997) % 24) as i32;
    let minute = ((index / 13) % 60) as i32;
    let second = (index % 60) as i32;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

fn random_code(index: usize, rng: &mut StdRng) -> String {
    let mut alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'];
    let len = alphabet.len();
    alphabet.rotate_left(index % len);
    let mut buf = String::with_capacity(6);
    for ch in alphabet.iter().take(3) {
        buf.push(*ch);
    }
    buf.push('-');
    buf.push_str(&Alphanumeric.sample_string(rng, 2));
    buf
}

fn main() {
    let mut rng = StdRng::seed_from_u64(0x5A7A_5015_2024);
    let tenants = ["alpha", "beta", "gamma", "delta", "omega"];
    let regions = ["americas", "emea", "apac", "africa", "antarctica", "orbit"];
    let segments = [
        "consumer",
        "enterprise",
        "public_sector",
        "startup",
        "partner",
    ];

    for idx in 0..=11 {
        let tenant = tenants[idx % tenants.len()];
        let region = regions[(idx / tenants.len()) % regions.len()];
        let segment = segments[(idx / regions.len()) % segments.len()];
        let quantity = (rng.gen_range(1..=5000) as i64) + ((idx % 7) as i64);
        let price = ((rng.gen_range(10_000..=750_000) as f64) / 100.0)
            * (1.0 + ((idx % 11) as f64) / 500.0);
        let discount = if idx % 9 == 0 {
            0.0
        } else {
            (rng.gen_range(0..=4_000) as f64) / 10_000.0
        };
        let net_amount = price * quantity as f64 * (1.0 - discount);
        let created_at = synthetic_timestamp(idx);
        let active = idx % 13 != 0;
        let description = format!(
            "{}:{}:{}:{}:{}",
            tenant,
            region,
            segment,
            idx % 997,
            random_code(idx, &mut rng)
        );

        if idx == 10 || idx == 11 {
            println!(
                "idx={idx} id={} region={} active={} discount={} net_amount={} created_at={} description={}",
                idx + 1,
                region,
                active,
                discount,
                net_amount,
                created_at,
                description
            );
        }
    }
}
