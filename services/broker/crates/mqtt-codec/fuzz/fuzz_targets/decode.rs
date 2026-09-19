#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use mqtt_codec::{decode, Limits, Version};

fuzz_target!(|data: &[u8]| {
    // Drain the buffer completely. Every iteration must either consume bytes
    // or stop, so a decoder that returns a packet without advancing shows up
    // as a hang rather than passing silently.
    for version in [Version::V311, Version::V5] {
        let mut buf = BytesMut::from(data);
        loop {
            let before = buf.len();
            match decode(&mut buf, version, &Limits::default()) {
                Ok(Some(_)) => {
                    assert!(
                        buf.len() < before,
                        "decode returned a packet without consuming"
                    );
                }
                Ok(None) | Err(_) => break,
            }
        }
    }
});
