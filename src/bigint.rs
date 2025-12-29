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
