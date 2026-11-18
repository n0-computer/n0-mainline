//! Manage tokens for remote client IPs.

use rand::Rng;
use sha1_smol::Sha1;
use std::{
    fmt::{self, Debug, Formatter},
    net::SocketAddrV4,
    time::Instant,
};

use tracing::trace;

const SECRET_SIZE: usize = 20;
const TOKEN_SIZE: usize = 4;

/// Tokens generator.
///
/// Read [BEP_0005](https://www.bittorrent.org/beps/bep_0005.html) for more information.
#[derive(Clone)]
pub struct Tokens {
    prev_secret: [u8; SECRET_SIZE],
    curr_secret: [u8; SECRET_SIZE],
    last_updated: Instant,
}

impl Debug for Tokens {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Tokens (_)")
    }
}

impl Tokens {
    /// Create a Tokens generator.
    pub fn new() -> Self {
        Tokens {
            prev_secret: random(),
            curr_secret: random(),
            last_updated: Instant::now(),
        }
    }

    // === Public Methods ===

    /// Returns `true` if the current secret needs to be updated after an interval.
    pub fn should_update(&self) -> bool {
        self.last_updated.elapsed() > crate::common::TOKEN_ROTATE_INTERVAL
    }

    /// Validate that the token was generated within the past 10 minutes
    pub fn validate(&mut self, address: SocketAddrV4, token: &[u8]) -> bool {
        let prev = self.internal_generate_token(address, self.prev_secret);
        let curr = self.internal_generate_token(address, self.curr_secret);

        token == curr || token == prev
    }

    /// Rotate the tokens secret.
    pub fn rotate(&mut self) {
        trace!("Rotating secrets");

        self.prev_secret = self.curr_secret;
        self.curr_secret = random();

        self.last_updated = Instant::now();
    }

    /// Generates a new token for a remote peer.
    pub fn generate_token(&mut self, address: SocketAddrV4) -> [u8; 4] {
        self.internal_generate_token(address, self.curr_secret)
    }

    // === Private Methods ===

    fn internal_generate_token(
        &mut self,
        address: SocketAddrV4,
        secret: [u8; SECRET_SIZE],
    ) -> [u8; TOKEN_SIZE] {
        // A keyed hash, as BEP 5 suggests. A CRC is linear, so the token for one IP would
        // give away the token for any other.
        let mut hasher = Sha1::new();
        hasher.update(&address.ip().octets());
        hasher.update(&secret);
        let digest = hasher.digest().bytes();

        let mut token = [0; TOKEN_SIZE];
        token.copy_from_slice(&digest[..TOKEN_SIZE]);
        token
    }
}

impl Default for Tokens {
    fn default() -> Self {
        Self::new()
    }
}

fn random() -> [u8; SECRET_SIZE] {
    let mut bytes = [0_u8; SECRET_SIZE];
    rand::rng().fill_bytes(&mut bytes);

    bytes
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn valid_tokens() {
        let mut tokens = Tokens::new();

        let address = SocketAddrV4::new([127, 0, 0, 1].into(), 6881);
        let token = tokens.generate_token(address);

        assert!(tokens.validate(address, &token))
    }

    #[test]
    fn token_of_one_ip_does_not_give_the_token_of_another() {
        use crc::{CRC_32_ISCSI, Crc};
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);

        let mut tokens = Tokens::new();
        let attacker = SocketAddrV4::new([203, 0, 113, 7].into(), 6881);
        let victim = SocketAddrV4::new([198, 51, 100, 9].into(), 6881);
        let token = u32::from_be_bytes(tokens.generate_token(attacker));

        // If the token were crc(ip || secret), this would forge the victim's token without
        // knowing the secret: for messages of equal length, crc is affine in its input.
        let mut delta = [0u8; 4 + SECRET_SIZE];
        for (d, (a, v)) in delta
            .iter_mut()
            .zip(attacker.ip().octets().iter().zip(victim.ip().octets()))
        {
            *d = a ^ v;
        }
        let forged = token ^ crc.checksum(&delta) ^ crc.checksum(&[0; 4 + SECRET_SIZE]);

        assert!(!tokens.validate(victim, &forged.to_be_bytes()));
    }
}
