#![deny(unsafe_code)]

pub const DATA_SHARDS: usize = 6;
pub const PARITY_SHARDS: usize = 4;
pub const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
const STRIPE_IDENTITY_DOMAIN: &[u8] = b"cairn/ec/stripe/v1";

/// Trusted metadata for one complete stripe.
///
/// The descriptor must come from the store that owns the object.  In
/// particular, callers must not build it from the bytes they are trying to
/// repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StripeDescriptor {
    object_id: [u8; 32],
    expected_checksums: [[u8; 32]; TOTAL_SHARDS],
}

impl StripeDescriptor {
    pub fn new(
        object_id: [u8; 32],
        expected_checksums: [[u8; 32]; TOTAL_SHARDS],
    ) -> Result<Self, EcError> {
        let descriptor = Self {
            object_id,
            expected_checksums,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn derived_object_id(expected_checksums: [[u8; 32]; TOTAL_SHARDS]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(STRIPE_IDENTITY_DOMAIN);
        for checksum in expected_checksums {
            hasher.update(&checksum);
        }
        *hasher.finalize().as_bytes()
    }

    fn validate(&self) -> Result<(), EcError> {
        if self.object_id != Self::derived_object_id(self.expected_checksums) {
            return Err(EcError::StripeDescriptorIdentityMismatch);
        }
        Ok(())
    }

    pub const fn object_id(&self) -> [u8; 32] {
        self.object_id
    }

    pub const fn expected_checksum(&self, position: usize) -> Option<[u8; 32]> {
        if position < TOTAL_SHARDS {
            Some(self.expected_checksums[position])
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardBuffer {
    position: usize,
    bytes: Vec<u8>,
    expected_checksum: [u8; 32],
}

impl ShardBuffer {
    pub fn new(position: usize, bytes: Vec<u8>) -> Self {
        let expected_checksum = *blake3::hash(&bytes).as_bytes();
        Self {
            position,
            bytes,
            expected_checksum,
        }
    }

    pub fn from_bytes(position: usize, bytes: Vec<u8>) -> Self {
        Self::new(position, bytes)
    }

    pub fn with_checksum(position: usize, bytes: Vec<u8>, expected_checksum: [u8; 32]) -> Self {
        Self {
            position,
            bytes,
            expected_checksum,
        }
    }

    pub const fn position(&self) -> usize {
        self.position
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
    InvalidShardPosition { position: usize },
    ShardPositionMismatch { expected: usize, actual: usize },
    EmptyShardSet,
    UnequalShardLengths,
    TooFewShards { available: usize, required: usize },
    MissingDataShard(usize),
    SingularMatrix,
    StripeDescriptorIdentityMismatch,
    StripeDescriptorMismatch { position: usize },
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

    pub fn reconstruct(
        &self,
        shards: &[Option<ShardBuffer>],
        descriptor: &StripeDescriptor,
    ) -> Result<Vec<Vec<u8>>, EcError> {
        self.validate_profile()?;
        descriptor.validate()?;
        if shards.len() != TOTAL_SHARDS {
            return Err(EcError::InvalidShardCount {
                expected: TOTAL_SHARDS,
                actual: shards.len(),
            });
        }
        for (expected, shard) in shards
            .iter()
            .enumerate()
            .filter_map(|(index, shard)| shard.as_ref().map(|shard| (index, shard)))
        {
            if shard.position() >= TOTAL_SHARDS {
                return Err(EcError::InvalidShardPosition {
                    position: shard.position(),
                });
            }
            if shard.position() != expected {
                return Err(EcError::ShardPositionMismatch {
                    expected,
                    actual: shard.position(),
                });
            }
        }
        let available: Vec<(usize, &[u8])> = shards
            .iter()
            .enumerate()
            .filter_map(|(index, shard)| {
                shard
                    .as_ref()
                    .filter(|value| {
                        descriptor
                            .expected_checksum(value.position())
                            .is_some_and(|checksum| {
                                *blake3::hash(value.bytes()).as_bytes() == checksum
                            })
                    })
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
        let parity = self.encode(&data)?;
        let stripe = data.into_iter().chain(parity).collect::<Vec<_>>();
        for (position, bytes) in stripe.iter().enumerate() {
            let checksum = *blake3::hash(bytes).as_bytes();
            if descriptor.expected_checksum(position) != Some(checksum) {
                return Err(EcError::StripeDescriptorMismatch { position });
            }
        }
        Ok(stripe[..DATA_SHARDS].to_vec())
    }

    pub fn reconstruct_all(
        &self,
        shards: &[Option<ShardBuffer>],
        descriptor: &StripeDescriptor,
    ) -> Result<Vec<Vec<u8>>, EcError> {
        Ok(self
            .repair(shards, descriptor)?
            .into_iter()
            .map(|shard| shard.bytes)
            .collect())
    }

    pub fn repair(
        &self,
        shards: &[Option<ShardBuffer>],
        descriptor: &StripeDescriptor,
    ) -> Result<Vec<ShardBuffer>, EcError> {
        descriptor.validate()?;
        let data = self.reconstruct(shards, descriptor)?;
        let parity = self.encode(&data)?;
        Ok(data
            .into_iter()
            .chain(parity)
            .enumerate()
            .map(|(position, bytes)| {
                ShardBuffer::with_checksum(
                    position,
                    bytes,
                    descriptor
                        .expected_checksum(position)
                        .expect("validated complete stripe descriptor"),
                )
            })
            .collect())
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
            let pivot_row = augmented[column].clone();
            for (value, pivot_value) in augmented[row].iter_mut().zip(pivot_row) {
                *value ^= gf_mul(factor, pivot_value);
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

    fn stripe(original: &[Vec<u8>], parity: &[Vec<u8>]) -> Vec<Option<ShardBuffer>> {
        original
            .iter()
            .cloned()
            .chain(parity.iter().cloned())
            .enumerate()
            .map(|(position, bytes)| Some(ShardBuffer::new(position, bytes)))
            .collect()
    }

    fn descriptor(original: &[Vec<u8>], parity: &[Vec<u8>]) -> StripeDescriptor {
        let checksums = original
            .iter()
            .chain(parity)
            .map(|bytes| *blake3::hash(bytes).as_bytes())
            .collect::<Vec<_>>();
        let checksums = checksums.try_into().expect("complete stripe descriptor");
        StripeDescriptor::new(StripeDescriptor::derived_object_id(checksums), checksums).unwrap()
    }

    #[test]
    fn profile_is_six_plus_four() {
        assert_eq!(ReedSolomon::new().profile(), EcProfile::SIX_PLUS_FOUR);
    }

    #[test]
    fn swapped_shards_are_rejected_before_decode() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let descriptor = descriptor(&original, &parity);
        let mut shards = stripe(&original, &parity);
        shards.swap(0, 1);

        assert_eq!(
            codec.reconstruct(&shards, &descriptor),
            Err(EcError::ShardPositionMismatch {
                expected: 0,
                actual: 1,
            })
        );
    }

    #[test]
    fn every_four_shard_erasure_pattern_recovers_data() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let descriptor = descriptor(&original, &parity);
        let mut shards = stripe(&original, &parity);
        for mask in 0..(1u16 << TOTAL_SHARDS) {
            if mask.count_ones() != 4 {
                continue;
            }
            for (index, shard) in shards.iter_mut().enumerate() {
                *shard = if mask & (1 << index) == 0 {
                    Some(ShardBuffer::new(
                        index,
                        if index < DATA_SHARDS {
                            original[index].clone()
                        } else {
                            parity[index - DATA_SHARDS].clone()
                        },
                    ))
                } else {
                    None
                };
            }
            assert_eq!(codec.reconstruct(&shards, &descriptor).unwrap(), original);
        }
    }

    #[test]
    fn five_missing_shards_are_not_recoverable() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let descriptor = descriptor(&original, &parity);
        let mut shards = stripe(&original, &parity);
        for shard in shards.iter_mut().take(5) {
            *shard = None;
        }
        assert!(matches!(
            codec.reconstruct(&shards, &descriptor),
            Err(EcError::TooFewShards {
                available: 5,
                required: 6
            })
        ));
    }

    #[test]
    fn corrupted_shard_is_excluded_and_repair_returns_all_shards() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let descriptor = descriptor(&original, &parity);
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
                Some(ShardBuffer::with_checksum(index, bytes, expected))
            })
            .collect();
        let corrupted = {
            let mut bytes = original[0].clone();
            bytes[0] ^= 1;
            ShardBuffer::with_checksum(0, bytes, *blake3::hash(&original[0]).as_bytes())
        };
        shards[0] = Some(corrupted);
        shards[1] = None;
        shards[2] = None;
        shards[3] = None;

        let recovered = codec.repair(&shards, &descriptor).unwrap();
        assert_eq!(recovered.len(), TOTAL_SHARDS);
        for (position, shard) in recovered.iter().enumerate() {
            assert_eq!(shard.position(), position);
            assert!(shard.is_valid());
        }
        assert_eq!(
            recovered.iter().map(ShardBuffer::bytes).collect::<Vec<_>>(),
            original
                .iter()
                .chain(parity.iter())
                .map(Vec::as_slice)
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn five_invalid_checksums_exceed_tolerance() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let descriptor = descriptor(&original, &parity);
        let mut shards = stripe(&original, &parity);
        for (position, shard) in shards.iter_mut().enumerate().take(PARITY_SHARDS + 1) {
            let mut bytes = shard.as_ref().unwrap().bytes().to_vec();
            bytes[0] ^= 1;
            *shard = Some(ShardBuffer::new(position, bytes));
        }

        assert_eq!(
            codec.repair(&shards, &descriptor),
            Err(EcError::TooFewShards {
                available: DATA_SHARDS - 1,
                required: DATA_SHARDS,
            })
        );
    }

    #[test]
    fn same_position_from_another_stripe_is_not_accepted() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let descriptor = descriptor(&original, &parity);
        let mut shards = stripe(&original, &parity);
        let other_stripe = (0..257).map(|offset| offset as u8 ^ 0xa5).collect();
        shards[0] = Some(ShardBuffer::new(0, other_stripe));
        for shard in shards.iter_mut().skip(DATA_SHARDS) {
            *shard = None;
        }

        assert_eq!(
            codec.reconstruct(&shards, &descriptor),
            Err(EcError::TooFewShards {
                available: DATA_SHARDS - 1,
                required: DATA_SHARDS,
            })
        );
    }

    #[test]
    fn self_signed_replacement_is_not_accepted() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let descriptor = descriptor(&original, &parity);
        let mut shards = stripe(&original, &parity);
        let mut replacement = original[0].clone();
        replacement[0] ^= 0xff;
        shards[0] = Some(ShardBuffer::new(0, replacement));
        for shard in shards.iter_mut().skip(DATA_SHARDS) {
            *shard = None;
        }

        assert_eq!(
            codec.reconstruct(&shards, &descriptor),
            Err(EcError::TooFewShards {
                available: DATA_SHARDS - 1,
                required: DATA_SHARDS,
            })
        );
    }

    #[test]
    fn mixed_shards_accepted_individually_but_not_as_one_stripe() {
        let codec = ReedSolomon::new();
        let first_data = data();
        let second_data = first_data
            .iter()
            .enumerate()
            .map(|(index, shard)| {
                shard
                    .iter()
                    .map(|byte| byte.wrapping_add(index as u8 + 1))
                    .collect()
            })
            .collect::<Vec<Vec<u8>>>();
        let second_parity = codec.encode(&second_data).unwrap();
        let mixed_checksums = first_data
            .iter()
            .chain(second_parity.iter())
            .map(|bytes| *blake3::hash(bytes).as_bytes())
            .collect::<Vec<_>>();
        let mixed_checksums = mixed_checksums
            .try_into()
            .expect("complete stripe descriptor");
        let descriptor = StripeDescriptor {
            object_id: StripeDescriptor::derived_object_id(mixed_checksums),
            expected_checksums: mixed_checksums,
        };
        let shards = first_data
            .iter()
            .cloned()
            .chain(second_parity.iter().cloned())
            .enumerate()
            .map(|(position, bytes)| Some(ShardBuffer::new(position, bytes)))
            .collect::<Vec<_>>();

        assert_eq!(
            codec.reconstruct(&shards, &descriptor),
            Err(EcError::StripeDescriptorMismatch {
                position: DATA_SHARDS
            })
        );
        assert_eq!(
            codec.repair(&shards, &descriptor),
            Err(EcError::StripeDescriptorMismatch {
                position: DATA_SHARDS
            })
        );
    }

    #[test]
    fn object_id_is_bound_to_expected_checksums() {
        let codec = ReedSolomon::new();
        let original = data();
        let parity = codec.encode(&original).unwrap();
        let checksums = original
            .iter()
            .chain(parity.iter())
            .map(|bytes| *blake3::hash(bytes).as_bytes())
            .collect::<Vec<_>>()
            .try_into()
            .expect("complete stripe descriptor");
        let object_a = StripeDescriptor::derived_object_id(checksums);
        let mut other_data = original.clone();
        other_data[0][0] ^= 1;
        let other_parity = codec.encode(&other_data).unwrap();
        let other_checksums = other_data
            .iter()
            .chain(other_parity.iter())
            .map(|bytes| *blake3::hash(bytes).as_bytes())
            .collect::<Vec<_>>()
            .try_into()
            .expect("complete stripe descriptor");
        let object_b = StripeDescriptor::derived_object_id(other_checksums);
        assert_ne!(object_a, object_b);

        assert_eq!(
            StripeDescriptor::new(object_b, checksums),
            Err(EcError::StripeDescriptorIdentityMismatch)
        );

        let malformed = StripeDescriptor {
            object_id: object_b,
            expected_checksums: checksums,
        };
        let shards = stripe(&original, &parity);
        assert_eq!(
            codec.reconstruct(&shards, &malformed),
            Err(EcError::StripeDescriptorIdentityMismatch)
        );
        assert_eq!(
            codec.repair(&shards, &malformed),
            Err(EcError::StripeDescriptorIdentityMismatch)
        );
    }
}
