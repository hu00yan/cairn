#![deny(unsafe_code)]

pub const DATA_SHARDS: usize = 6;
pub const PARITY_SHARDS: usize = 4;
pub const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardBuffer {
    bytes: Vec<u8>,
    expected_checksum: [u8; 32],
}

impl ShardBuffer {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let expected_checksum = *blake3::hash(&bytes).as_bytes();
        Self {
            bytes,
            expected_checksum,
        }
    }

    pub fn with_checksum(bytes: Vec<u8>, expected_checksum: [u8; 32]) -> Self {
        Self {
            bytes,
            expected_checksum,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn expected_checksum(&self) -> [u8; 32] {
        self.expected_checksum
    }

    pub fn is_valid(&self) -> bool {
        *blake3::hash(&self.bytes).as_bytes() == self.expected_checksum
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EcProfile {
    pub data_shards: usize,
    pub parity_shards: usize,
}

impl EcProfile {
    pub const SIX_PLUS_FOUR: Self = Self {
        data_shards: DATA_SHARDS,
        parity_shards: PARITY_SHARDS,
    };
}

impl Default for EcProfile {
    fn default() -> Self {
        Self::SIX_PLUS_FOUR
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EcError {
    UnsupportedProfile,
    InvalidShardCount { expected: usize, actual: usize },
    EmptyShardSet,
    UnequalShardLengths,
    TooFewShards { available: usize, required: usize },
    MissingDataShard(usize),
    SingularMatrix,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ReedSolomon {
    profile: EcProfile,
}

impl ReedSolomon {
    pub const fn new() -> Self {
        Self {
            profile: EcProfile::SIX_PLUS_FOUR,
        }
    }

    pub const fn with_profile(profile: EcProfile) -> Result<Self, EcError> {
        if profile.data_shards != DATA_SHARDS || profile.parity_shards != PARITY_SHARDS {
            return Err(EcError::UnsupportedProfile);
        }
        Ok(Self { profile })
    }

    pub const fn profile(&self) -> EcProfile {
        self.profile
    }

    pub fn encode(&self, data: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, EcError> {
        self.validate_profile()?;
        if data.len() != DATA_SHARDS {
            return Err(EcError::InvalidShardCount {
                expected: DATA_SHARDS,
                actual: data.len(),
            });
        }
        let len = data.first().ok_or(EcError::EmptyShardSet)?.len();
        if data.iter().any(|shard| shard.len() != len) {
            return Err(EcError::UnequalShardLengths);
        }
        let matrix = generator_matrix()?;
        let mut parity = vec![vec![0; len]; PARITY_SHARDS];
        for (parity_index, output) in parity.iter_mut().enumerate() {
            for (data_index, input) in data.iter().enumerate() {
                let coefficient = matrix[DATA_SHARDS + parity_index][data_index];
                for (dst, src) in output.iter_mut().zip(input) {
                    *dst ^= gf_mul(coefficient, *src);
                }
            }
        }
        Ok(parity)
    }

    pub fn reconstruct(&self, shards: &[Option<ShardBuffer>]) -> Result<Vec<Vec<u8>>, EcError> {
        self.validate_profile()?;
        if shards.len() != TOTAL_SHARDS {
            return Err(EcError::InvalidShardCount {
                expected: TOTAL_SHARDS,
                actual: shards.len(),
            });
        }
        let available: Vec<(usize, &[u8])> = shards
            .iter()
            .enumerate()
            .filter_map(|(index, shard)| {
                shard
                    .as_ref()
                    .filter(|value| value.is_valid())
                    .map(|value| (index, value.bytes()))
            })
            .collect();
        if available.len() < DATA_SHARDS {
            return Err(EcError::TooFewShards {
                available: available.len(),
                required: DATA_SHARDS,
            });
        }
        let len = available[0].1.len();
        if available.iter().any(|(_, shard)| shard.len() != len) {
            return Err(EcError::UnequalShardLengths);
        }

        let selected = &available[..DATA_SHARDS];
        let generator = generator_matrix()?;
        let mut matrix = vec![vec![0; DATA_SHARDS]; DATA_SHARDS];
        for (row, (shard_index, _)) in selected.iter().enumerate() {
            for column in 0..DATA_SHARDS {
                matrix[row][column] = generator[*shard_index][column];
            }
        }
        let inverse = invert(&matrix)?;
        let mut data = vec![vec![0; len]; DATA_SHARDS];
        for (output_index, output) in data.iter_mut().enumerate() {
            for (selected_index, (_, input)) in selected.iter().enumerate() {
                let coefficient = inverse[output_index][selected_index];
                for (dst, src) in output.iter_mut().zip(input.iter()) {
                    *dst ^= gf_mul(coefficient, *src);
                }
            }
        }
        Ok(data)
    }

    pub fn reconstruct_all(&self, shards: &[Option<ShardBuffer>]) -> Result<Vec<Vec<u8>>, EcError> {
        let data = self.reconstruct(shards)?;
        let parity = self.encode(&data)?;
        Ok(data.into_iter().chain(parity).collect())
    }

    fn validate_profile(&self) -> Result<(), EcError> {
        if self.profile == EcProfile::SIX_PLUS_FOUR {
            Ok(())
        } else {
            Err(EcError::UnsupportedProfile)
        }
    }
}

fn generator_matrix() -> Result<Vec<Vec<u8>>, EcError> {
    let vandermonde: Vec<Vec<u8>> = (0..TOTAL_SHARDS)
        .map(|row| {
            let base = (row + 1) as u8;
            (0..DATA_SHARDS)
                .map(|column| gf_pow(base, column as u8))
                .collect()
        })
        .collect();
    let inverse = invert(&vandermonde[..DATA_SHARDS])?;
    let mut generator = vec![vec![0; DATA_SHARDS]; TOTAL_SHARDS];
    for row in 0..TOTAL_SHARDS {
        for column in 0..DATA_SHARDS {
            for (index, inverse_row) in inverse.iter().enumerate() {
                generator[row][column] ^= gf_mul(vandermonde[row][index], inverse_row[column]);
            }
        }
    }
    Ok(generator)
}

fn invert(input: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, EcError> {
    let n = input.len();
    let mut augmented = vec![vec![0; n * 2]; n];
    for row in 0..n {
        if input[row].len() != n {
            return Err(EcError::SingularMatrix);
        }
        augmented[row][..n].copy_from_slice(&input[row]);
        augmented[row][n + row] = 1;
    }
    for column in 0..n {
        let pivot = (column..n)
            .find(|&row| augmented[row][column] != 0)
            .ok_or(EcError::SingularMatrix)?;
        augmented.swap(column, pivot);
        let scale = gf_inv(augmented[column][column]);
        for value in &mut augmented[column] {
            *value = gf_mul(*value, scale);
        }
        for row in 0..n {
            if row == column {
                continue;
            }
            let factor = augmented[row][column];
            if factor == 0 {
                continue;
            }
            for value in 0..n * 2 {
                augmented[row][value] ^= gf_mul(factor, augmented[column][value]);
            }
        }
    }
    Ok(augmented.into_iter().map(|row| row[n..].to_vec()).collect())
}

fn gf_mul(mut left: u8, mut right: u8) -> u8 {
    let mut result = 0;
    while right != 0 {
        if right & 1 != 0 {
            result ^= left;
        }
        let carry = left & 0x80 != 0;
        left <<= 1;
        if carry {
            left ^= 0x1d;
        }
        right >>= 1;
    }
    result
}

fn gf_pow(mut value: u8, mut power: u8) -> u8 {
    let mut result = 1;
    while power != 0 {
        if power & 1 != 0 {
            result = gf_mul(result, value);
        }
        value = gf_mul(value, value);
        power >>= 1;
    }
    result
}

fn gf_inv(value: u8) -> u8 {
    debug_assert_ne!(value, 0);
    gf_pow(value, 254)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> Vec<Vec<u8>> {
        (0..DATA_SHARDS)
            .map(|index| {
                (0..257)
                    .map(|offset| (index as u8).wrapping_mul(19).wrapping_add(offset as u8))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn profile_is_six_plus_four() {
        assert_eq!(ReedSolomon::new().profile(), EcProfile::SIX_PLUS_FOUR);
    }

    #[test]
    fn every_four_shard_erasure_pattern_recovers_data() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let mut shards: Vec<Option<ShardBuffer>> = original
            .iter()
            .cloned()
            .map(|shard| Some(ShardBuffer::from_bytes(shard)))
            .chain(
                parity
                    .iter()
                    .cloned()
                    .map(|shard| Some(ShardBuffer::from_bytes(shard))),
            )
            .collect();
        for mask in 0..(1u16 << TOTAL_SHARDS) {
            if mask.count_ones() != 4 {
                continue;
            }
            for (index, shard) in shards.iter_mut().enumerate() {
                *shard = if mask & (1 << index) == 0 {
                    Some(ShardBuffer::from_bytes(if index < DATA_SHARDS {
                        original[index].clone()
                    } else {
                        parity[index - DATA_SHARDS].clone()
                    }))
                } else {
                    None
                };
            }
            assert_eq!(codec.reconstruct(&shards).unwrap(), original);
        }
    }

    #[test]
    fn five_missing_shards_are_not_recoverable() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let mut shards: Vec<Option<ShardBuffer>> = original
            .into_iter()
            .map(|shard| Some(ShardBuffer::from_bytes(shard)))
            .chain(
                parity
                    .into_iter()
                    .map(|shard| Some(ShardBuffer::from_bytes(shard))),
            )
            .collect();
        for shard in shards.iter_mut().take(5) {
            *shard = None;
        }
        assert!(matches!(
            codec.reconstruct(&shards),
            Err(EcError::TooFewShards {
                available: 5,
                required: 6
            })
        ));
    }

    #[test]
    fn invalid_checksum_shards_are_excluded_and_repair_returns_all_shards() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let mut shards: Vec<Option<ShardBuffer>> = original
            .iter()
            .cloned()
            .chain(parity.iter().cloned())
            .enumerate()
            .map(|(index, bytes)| {
                let expected = if index < DATA_SHARDS {
                    *blake3::hash(&original[index]).as_bytes()
                } else {
                    *blake3::hash(&parity[index - DATA_SHARDS]).as_bytes()
                };
                Some(ShardBuffer::with_checksum(bytes, expected))
            })
            .collect();
        let corrupted = {
            let mut bytes = original[0].clone();
            bytes[0] ^= 1;
            ShardBuffer::with_checksum(bytes, *blake3::hash(&original[0]).as_bytes())
        };
        shards[0] = Some(corrupted);
        shards[1] = None;
        shards[2] = None;
        shards[3] = None;

        let recovered = codec.reconstruct_all(&shards).unwrap();
        assert_eq!(&recovered[..DATA_SHARDS], original.as_slice());
        assert_eq!(&recovered[DATA_SHARDS..], parity.as_slice());
    }
}
