// Print the blake2b-224 keyhash for a 32 or 64-byte hex Ed25519 secret key.
//   cargo run --example keyhash -- <hex>
use pallas_crypto::hash::Hasher;
use pallas_crypto::key::ed25519::{SecretKey, SecretKeyExtended};

fn main() {
    let hex_str = std::env::args().nth(1).expect("usage: keyhash <hex>");
    let bytes = hex::decode(&hex_str).expect("invalid hex");
    let pk = match bytes.len() {
        32 => {
            let arr: [u8; 32] = bytes.try_into().unwrap();
            SecretKey::from(arr).public_key()
        }
        64 => {
            let arr: [u8; 64] = bytes.try_into().unwrap();
            SecretKeyExtended::from_bytes(arr).unwrap().public_key()
        }
        n => panic!("expected 32 or 64 bytes, got {n}"),
    };
    let pk_bytes: [u8; 32] = pk.as_ref().try_into().unwrap();
    let hash = Hasher::<224>::hash(&pk_bytes);
    println!("{}", hex::encode(hash.as_ref()));
}
