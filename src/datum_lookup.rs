use std::{collections::BTreeMap, sync::Arc};

use pallas_crypto::hash::Hasher;
use pallas_primitives::{DatumHash, KeepRaw};
use pallas_traverse::MultiEraTx;
use plutus_parser::PlutusData;

#[derive(Debug, Clone, Default)]
pub struct DatumLookup {
    datums: Arc<BTreeMap<DatumHash, PlutusData>>,
    bytes: Arc<BTreeMap<DatumHash, Vec<u8>>>,
}

impl DatumLookup {
    pub fn add_metadata_datum(&mut self, datum: (Vec<u8>, PlutusData)) {
        let hash = Hasher::<256>::hash(&datum.0);
        let bytes = Arc::make_mut(&mut self.bytes);
        bytes.insert(hash, datum.0);
        let datums = Arc::make_mut(&mut self.datums);
        datums.insert(hash, datum.1);
    }

    #[cfg(test)]
    pub fn contains_metadata_datum(&self, hash: DatumHash) -> bool {
        self.datums.contains_key(&hash)
    }

    pub fn for_tx<'a>(&'a self, tx: &MultiEraTx<'a>) -> ScopedDatumLookup<'a> {
        let data = tx
            .plutus_data()
            .iter()
            .map(|datum| {
                let hash = Hasher::<256>::hash(datum.raw_cbor());
                (hash, datum.clone())
            })
            .collect();
        ScopedDatumLookup { root: self, data }
    }

    pub fn for_persisted_txo<'a>(
        &'a self,
        datum: Option<KeepRaw<'a, PlutusData>>,
    ) -> ScopedDatumLookup<'a> {
        let mut data = BTreeMap::new();
        if let Some(datum) = datum {
            let hash = Hasher::<256>::hash(datum.raw_cbor());
            data.insert(hash, datum);
        }
        ScopedDatumLookup { root: self, data }
    }
}

pub struct ScopedDatumLookup<'a> {
    root: &'a DatumLookup,
    data: BTreeMap<DatumHash, KeepRaw<'a, PlutusData>>,
}

impl<'a> ScopedDatumLookup<'a> {
    pub fn lookup_datum(&'a self, hash: DatumHash) -> Option<&'a PlutusData> {
        if let Some(datum) = self.data.get(&hash) {
            return Some(datum);
        }
        self.root.datums.get(&hash)
    }

    pub fn lookup_bytes(&self, hash: DatumHash) -> Option<Vec<u8>> {
        if let Some(datum) = self.data.get(&hash) {
            return Some(datum.raw_cbor().to_vec());
        }
        self.root.bytes.get(&hash).cloned()
    }
}
