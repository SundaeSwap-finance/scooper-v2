use num_traits::cast::ToPrimitive;
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
}

impl fmt::Display for BigInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.0)
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

impl std::ops::Sub for BigInt {
    type Output = BigInt;
    fn sub(self, other: BigInt) -> BigInt {
        Self(self.0 - other.0)
    }
}

impl std::ops::SubAssign for BigInt {
    fn sub_assign(&mut self, other: BigInt) {
        self.0 -= other.0
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
                let n = num_bigint::BigUint::from_bytes_be(&bytes);
                Ok(BigInt(num_bigint::BigInt::from_biguint(
                    num_bigint::Sign::Minus,
                    n,
                )))
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
                let bytes = big_uint.to_bytes_be();
                PlutusData::BigInt(pallas_primitives::BigInt::BigNInt(bytes.into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BigInt;
    use plutus_parser::AsPlutus;

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
            x = x * &n;
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
            x = x * &n;
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
