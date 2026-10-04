use num_traits::{ConstZero, Num, One, Signed, Zero, cast::ToPrimitive};
use pallas_primitives::PlutusData;
use plutus_parser::AsPlutus;
use std::fmt;

#[derive(Eq, Ord, PartialEq, PartialOrd, Clone, Debug)]
pub struct BigInt(num_bigint::BigInt);

impl BigInt {
    pub fn unwrap(self) -> num_bigint::BigInt {
        self.0
    }

    pub fn to_f64(&self) -> Option<f64> {
        self.0.to_f64()
    }

    /// Greatest common divisor (Euclidean). Returns `|a|` when `b == 0`.
    pub fn gcd(&self, other: &BigInt) -> BigInt {
        let mut a = self.0.clone();
        let mut b = other.0.clone();
        while !b.is_zero() {
            let r = &a % &b;
            a = b;
            b = r;
        }
        BigInt(a.abs())
    }
}

impl fmt::Display for BigInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<num_bigint::BigInt> for BigInt {
    fn from(i: num_bigint::BigInt) -> Self {
        BigInt(i)
    }
}

impl From<i32> for BigInt {
    fn from(i: i32) -> Self {
        Self(num_bigint::BigInt::from(i))
    }
}

impl From<i64> for BigInt {
    fn from(i: i64) -> Self {
        Self(num_bigint::BigInt::from(i))
    }
}

impl From<u64> for BigInt {
    fn from(u: u64) -> Self {
        Self(num_bigint::BigInt::from(u))
    }
}

impl From<i128> for BigInt {
    fn from(i: i128) -> Self {
        Self(num_bigint::BigInt::from(i))
    }
}

impl std::ops::Add for BigInt {
    type Output = BigInt;
    fn add(self, other: BigInt) -> BigInt {
        Self(self.0 + other.0)
    }
}

impl std::ops::Add<&BigInt> for &BigInt {
    type Output = BigInt;
    fn add(self, other: &BigInt) -> BigInt {
        BigInt(&self.0 + &other.0)
    }
}

impl std::ops::Add<&BigInt> for BigInt {
    type Output = BigInt;
    fn add(self, other: &BigInt) -> BigInt {
        Self(self.0 + &other.0)
    }
}

impl std::ops::Add<BigInt> for &BigInt {
    type Output = BigInt;
    fn add(self, other: BigInt) -> BigInt {
        BigInt(&self.0 + other.0)
    }
}

impl std::ops::AddAssign for BigInt {
    fn add_assign(&mut self, other: BigInt) {
        self.0 += other.0
    }
}

impl std::ops::AddAssign<&BigInt> for BigInt {
    fn add_assign(&mut self, other: &BigInt) {
        self.0 += &other.0
    }
}
impl std::ops::Sub for BigInt {
    type Output = BigInt;
    fn sub(self, other: BigInt) -> BigInt {
        Self(self.0 - other.0)
    }
}

impl std::ops::Sub<&BigInt> for &BigInt {
    type Output = BigInt;
    fn sub(self, other: &BigInt) -> BigInt {
        BigInt(&self.0 - &other.0)
    }
}

impl std::ops::Sub<&BigInt> for BigInt {
    type Output = BigInt;
    fn sub(self, other: &BigInt) -> BigInt {
        Self(self.0 - &other.0)
    }
}

impl std::ops::Sub<BigInt> for &BigInt {
    type Output = BigInt;
    fn sub(self, other: BigInt) -> BigInt {
        BigInt(&self.0 - other.0)
    }
}

impl std::ops::SubAssign for BigInt {
    fn sub_assign(&mut self, other: BigInt) {
        self.0 -= other.0
    }
}

impl std::ops::SubAssign<&BigInt> for BigInt {
    fn sub_assign(&mut self, other: &BigInt) {
        self.0 -= &other.0
    }
}

impl std::ops::Mul for BigInt {
    type Output = BigInt;
    fn mul(self, other: BigInt) -> BigInt {
        BigInt(&self.0 * &other.0)
    }
}

impl std::ops::Mul<&BigInt> for &BigInt {
    type Output = BigInt;
    fn mul(self, other: &BigInt) -> BigInt {
        BigInt(&self.0 * &other.0)
    }
}

impl std::ops::Mul<&BigInt> for BigInt {
    type Output = BigInt;
    fn mul(self, other: &BigInt) -> BigInt {
        BigInt(&self.0 * &other.0)
    }
}

impl std::ops::Mul<BigInt> for &BigInt {
    type Output = BigInt;
    fn mul(self, other: BigInt) -> BigInt {
        BigInt(&self.0 * &other.0)
    }
}

impl std::ops::MulAssign for BigInt {
    fn mul_assign(&mut self, other: BigInt) {
        self.0 *= other.0
    }
}

impl std::ops::MulAssign<&BigInt> for BigInt {
    fn mul_assign(&mut self, other: &BigInt) {
        self.0 *= &other.0
    }
}

impl std::ops::Div for BigInt {
    type Output = BigInt;
    fn div(self, rhs: Self) -> Self::Output {
        Self(self.0 / rhs.0)
    }
}

impl std::ops::Div<&BigInt> for &BigInt {
    type Output = BigInt;
    fn div(self, rhs: &BigInt) -> Self::Output {
        BigInt(&self.0 / &rhs.0)
    }
}

impl std::ops::Div<&BigInt> for BigInt {
    type Output = BigInt;
    fn div(self, rhs: &BigInt) -> Self::Output {
        BigInt(self.0 / &rhs.0)
    }
}

impl std::ops::Div<BigInt> for &BigInt {
    type Output = BigInt;
    fn div(self, rhs: BigInt) -> Self::Output {
        BigInt(&self.0 / rhs.0)
    }
}

impl std::ops::DivAssign for BigInt {
    fn div_assign(&mut self, rhs: Self) {
        self.0 /= rhs.0;
    }
}

impl std::ops::DivAssign<&BigInt> for BigInt {
    fn div_assign(&mut self, rhs: &BigInt) {
        self.0 /= &rhs.0;
    }
}

impl std::ops::Rem for BigInt {
    type Output = BigInt;
    fn rem(self, rhs: Self) -> Self::Output {
        Self(self.0 % rhs.0)
    }
}

impl std::ops::Rem<&BigInt> for &BigInt {
    type Output = BigInt;
    fn rem(self, rhs: &BigInt) -> Self::Output {
        BigInt(&self.0 % &rhs.0)
    }
}

impl std::ops::Rem<&BigInt> for BigInt {
    type Output = BigInt;
    fn rem(self, rhs: &BigInt) -> Self::Output {
        BigInt(self.0 % &rhs.0)
    }
}

impl std::ops::Rem<BigInt> for &BigInt {
    type Output = BigInt;
    fn rem(self, rhs: BigInt) -> Self::Output {
        BigInt(&self.0 % rhs.0)
    }
}

impl std::ops::RemAssign for BigInt {
    fn rem_assign(&mut self, rhs: Self) {
        self.0 %= rhs.0;
    }
}

impl std::ops::Neg for BigInt {
    type Output = BigInt;
    fn neg(self) -> Self::Output {
        Self(self.0.neg())
    }
}

impl std::ops::Neg for &BigInt {
    type Output = BigInt;
    fn neg(self) -> Self::Output {
        -self.clone()
    }
}

impl Zero for BigInt {
    fn zero() -> Self {
        Self(num_bigint::BigInt::zero())
    }

    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl ConstZero for BigInt {
    const ZERO: Self = Self(num_bigint::BigInt::ZERO);
}

impl One for BigInt {
    fn one() -> Self {
        Self(num_bigint::BigInt::one())
    }
}

impl Num for BigInt {
    type FromStrRadixErr = num_bigint::ParseBigIntError;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        Ok(Self(num_bigint::BigInt::from_str_radix(str, radix)?))
    }
}

impl Signed for BigInt {
    fn abs(&self) -> Self {
        Self(self.0.abs())
    }

    fn abs_sub(&self, other: &Self) -> Self {
        Self(self.0.abs_sub(&other.0))
    }

    fn signum(&self) -> Self {
        Self(self.0.signum())
    }

    fn is_positive(&self) -> bool {
        self.0.is_positive()
    }

    fn is_negative(&self) -> bool {
        self.0.is_negative()
    }
}

impl serde::Serialize for BigInt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Ok(n) = self.0.clone().try_into() as Result<i128, _> {
            return serializer.serialize_i128(n);
        }
        Err(serde::ser::Error::custom("BigInt out of i128 range"))
    }
}

impl AsPlutus for BigInt {
    fn from_plutus(data: PlutusData) -> Result<Self, plutus_parser::DecodeError> {
        let b: pallas_primitives::BigInt = AsPlutus::from_plutus(data)?;
        match b {
            pallas_primitives::BigInt::Int(i) => {
                Ok(BigInt(num_bigint::BigInt::from(Into::<i128>::into(i.0))))
            }
            pallas_primitives::BigInt::BigUInt(bytes) => {
                let n = num_bigint::BigUint::from_bytes_be(&bytes);
                Ok(BigInt(num_bigint::BigInt::from_biguint(
                    num_bigint::Sign::Plus,
                    n,
                )))
            }
            pallas_primitives::BigInt::BigNInt(bytes) => {
                // CBOR tag 3 (RFC 8949 s3.4.3): the payload `n` encodes the
                // value `-1 - n`, NOT `-n`. Reading it as `-n` makes every
                // large negative integer one too high.
                let n = num_bigint::BigUint::from_bytes_be(&bytes);
                let n = num_bigint::BigInt::from(n) + num_bigint::BigInt::from(1u8);
                Ok(BigInt(-n))
            }
        }
    }
    fn to_plutus(self) -> PlutusData {
        let self_as_i128: Result<i128, _> = self.0.clone().try_into();
        if let Ok(u) = self_as_i128 {
            let self_as_cbor_int: Result<minicbor::data::Int, _> = u.try_into();
            if let Ok(u) = self_as_cbor_int {
                return PlutusData::BigInt(pallas_primitives::BigInt::Int(pallas_primitives::Int(
                    u,
                )));
            }
        }
        let (sign, big_uint) = self.0.into_parts();
        match sign {
            num_bigint::Sign::Plus => {
                let bytes = big_uint.to_bytes_be();
                PlutusData::BigInt(pallas_primitives::BigInt::BigUInt(bytes.into()))
            }
            num_bigint::Sign::NoSign => {
                unreachable!()
            }
            num_bigint::Sign::Minus => {
                // The payload is `-1 - v`, i.e. `|v| - 1`. Writing `|v|` put
                // a value one MORE negative than intended on chain for every
                // negative integer too big for the 64-bit form. It cost a
                // mainnet stableswap withdrawal: the scooper resolved
                // target_delta_d = -7347631365219459674054077, serialised it
                // as -...078, and the module refused every submission with
                // `(D + t) * before_lp >= D * after_lp` false. The scooper
                // decoded its own output symmetrically, so the error was
                // invisible to a round trip.
                let bytes = (big_uint - 1u32).to_bytes_be();
                PlutusData::BigInt(pallas_primitives::BigInt::BigNInt(bytes.into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BigInt;
    use plutus_parser::AsPlutus;

    /// CBOR tag 3 carries `-1 - n`, so the ledger and every other codec read
    /// the payload one lower than the magnitude. Pin the bytes, not just a
    /// round trip: the encoder and decoder were wrong in the same direction,
    /// so a round trip agreed with itself and disagreed with the chain.
    #[test]
    fn large_negative_matches_cbor_tag3_semantics() {
        use num_traits::Num;
        // The mainnet stableswap withdraw target that was refused on chain.
        let v = num_bigint::BigInt::from_str_radix("-7347631365219459674054077", 10).unwrap();
        let mut buf = vec![];
        minicbor::encode(AsPlutus::to_plutus(BigInt(v.clone())), &mut buf).unwrap();
        let hex = hex::encode(&buf);
        // tag 3, 11-byte payload = |v| - 1, exactly what blaze produced.
        assert!(
            hex.contains("c34b0613ebe4f9ffafa04a9dbc"),
            "expected tag-3 payload |v|-1, got {hex}",
        );
        assert!(
            !hex.contains("c34b0613ebe4f9ffafa04a9dbd"),
            "payload must not be |v|: {hex}",
        );
    }

    #[test]
    fn large_negative_roundtrips() {
        use num_traits::Num;
        for s in [
            "-7347631365219459674054077",
            "-18446744073709551616",
            "-18446744073709551617",
            "-1",
            "-340282366920938463463374607431768211456",
        ] {
            let v = BigInt(num_bigint::BigInt::from_str_radix(s, 10).unwrap());
            let mut buf = vec![];
            minicbor::encode(AsPlutus::to_plutus(v.clone()), &mut buf).unwrap();
            let back: BigInt = AsPlutus::from_plutus(minicbor::decode(&buf).unwrap()).unwrap();
            assert_eq!(v, back, "round trip for {s}");
        }
    }

    /// Decoding alone, against a payload written by a correct encoder.
    #[test]
    fn decodes_tag3_payload_as_minus_one_minus_n() {
        use num_traits::Num;
        let bytes = hex::decode("c34b0613ebe4f9ffafa04a9dbc").unwrap();
        let got: BigInt = AsPlutus::from_plutus(minicbor::decode(&bytes).unwrap()).unwrap();
        assert_eq!(
            got,
            BigInt(num_bigint::BigInt::from_str_radix("-7347631365219459674054077", 10).unwrap()),
        );
    }

    #[test]
    fn bigint_roundtrip_small() {
        let x = BigInt::from(123);
        let mut byte_buf = vec![];
        let pd = AsPlutus::to_plutus(x.clone());
        minicbor::encode(&pd, &mut byte_buf).unwrap();
        let pd_from = minicbor::decode(&byte_buf).unwrap();
        let big_int_from = AsPlutus::from_plutus(pd_from).unwrap();
        assert_eq!(x, big_int_from);
    }

    #[test]
    fn bigint_roundtrip_big_pos() {
        let mut x = BigInt::from(1);
        let n = BigInt::from(256);
        for _ in 0..10 {
            x *= &n;
        }
        let u64_max = BigInt::from(u64::MAX);
        assert!(x > u64_max);
        let mut byte_buf = vec![];
        let pd = AsPlutus::to_plutus(x.clone());
        minicbor::encode(&pd, &mut byte_buf).unwrap();
        let pd_from = minicbor::decode(&byte_buf).unwrap();
        let big_int_from = AsPlutus::from_plutus(pd_from).unwrap();
        assert_eq!(x, big_int_from);
    }

    #[test]
    fn bigint_roundtrip_big_neg() {
        let mut x = BigInt::from(1);
        let n = BigInt::from(256);
        for _ in 0..11 {
            x *= &n;
        }
        x *= BigInt::from(-1);
        let neg_u64_max = BigInt::from(u64::MAX) * BigInt::from(-1);
        assert!(x < neg_u64_max);
        let mut byte_buf = vec![];
        let pd = AsPlutus::to_plutus(x.clone());
        minicbor::encode(&pd, &mut byte_buf).unwrap();
        let pd_from = minicbor::decode(&byte_buf).unwrap();
        let big_int_from = AsPlutus::from_plutus(pd_from).unwrap();
        assert_eq!(x, big_int_from);
    }
}
