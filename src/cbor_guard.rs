//! A depth bound on untrusted CBOR, applied before it reaches a decoder.
//!
//! `PlutusData`'s decoder recurses once per nesting level and has no depth
//! limit of its own. Rust cannot catch a stack overflow: the guard page
//! faults and the process aborts, so one malformed request takes down the
//! whole scooper rather than one connection. Measured on a 2 MB stack, which
//! is the tokio worker default: 3000 levels decode, 4000 abort. About 8 KB of
//! request body is enough.
//!
//! So the depth is measured first, without recursion, and the decoder only
//! ever sees input already known to be shallow.
//!
//! This walks the encoding with an explicit stack. It is deliberately
//! permissive about everything that is not depth: malformed input that it
//! accepts is still rejected by the real decoder a moment later, and the only
//! property this function must have is that it never recurses, always
//! advances, and never reports a depth lower than the truth.

/// Greatest nesting depth any accepted item may have.
///
/// A signed strategy execution nests on the order of ten levels. 128 leaves
/// room for the shape to grow and is still more than an order of magnitude
/// below the depth that overflows the stack.
pub const MAX_CBOR_DEPTH: usize = 128;

#[derive(Debug, PartialEq, Eq)]
pub enum DepthError {
    /// Nesting went past the limit. Carries the depth reached.
    TooDeep(usize),
    /// The bytes ran out mid-item. Reported so the caller can say "truncated"
    /// rather than "too deep", but either way the decoder is not invoked.
    Truncated,
}

impl std::fmt::Display for DepthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DepthError::TooDeep(d) => {
                write!(f, "CBOR nests {d} levels, limit is {MAX_CBOR_DEPTH}")
            }
            DepthError::Truncated => write!(f, "CBOR ends mid-item"),
        }
    }
}

impl std::error::Error for DepthError {}

enum Frame {
    /// A container with a known number of items still to come.
    Definite(u64),
    /// A container that runs until a break byte.
    Indefinite,
}

/// Measure the nesting depth of the first CBOR item in `bytes`.
///
/// Returns the greatest depth reached, or an error. Never recurses.
pub fn check_depth(bytes: &[u8], max_depth: usize) -> Result<usize, DepthError> {
    let mut pos = 0usize;
    let mut stack: Vec<Frame> = Vec::new();
    let mut deepest = 0usize;

    // Read a big-endian argument of `n` bytes following the header.
    let read_arg = |pos: &mut usize, n: usize| -> Option<u64> {
        let end = pos.checked_add(n)?;
        let slice = bytes.get(*pos..end)?;
        *pos = end;
        let mut v = 0u64;
        for b in slice {
            v = (v << 8) | u64::from(*b);
        }
        Some(v)
    };

    loop {
        let Some(&header) = bytes.get(pos) else {
            return Err(DepthError::Truncated);
        };
        pos += 1;
        let major = header >> 5;
        let ai = header & 0x1f;

        // A break closes the innermost indefinite container.
        if header == 0xff {
            match stack.pop() {
                Some(_) => {
                    complete_item(&mut stack);
                    if stack.is_empty() {
                        return Ok(deepest);
                    }
                    continue;
                }
                // A break with nothing open is malformed. Stop and let the
                // decoder produce the error message.
                None => return Ok(deepest),
            }
        }

        // Decode the header argument. 28..=30 are reserved; treat them as
        // malformed and hand the bytes on rather than guessing a length.
        let arg = match ai {
            0..=23 => Some(u64::from(ai)),
            24 => read_arg(&mut pos, 1),
            25 => read_arg(&mut pos, 2),
            26 => read_arg(&mut pos, 4),
            27 => read_arg(&mut pos, 8),
            31 => None, // indefinite
            _ => return Ok(deepest),
        };
        if ai <= 27 && arg.is_none() {
            return Err(DepthError::Truncated);
        }

        let opened = match major {
            // Unsigned and negative integers carry no payload past the head.
            0 | 1 => None,
            // Strings: skip the payload, or open an indefinite chunk run.
            2 | 3 => match arg {
                Some(len) => {
                    let len = usize::try_from(len).map_err(|_| DepthError::Truncated)?;
                    pos = pos.checked_add(len).ok_or(DepthError::Truncated)?;
                    if pos > bytes.len() {
                        return Err(DepthError::Truncated);
                    }
                    None
                }
                None => Some(Frame::Indefinite),
            },
            // Arrays and maps. A map's argument counts pairs, so it holds
            // twice that many items.
            4 => Some(match arg {
                Some(n) => Frame::Definite(n),
                None => Frame::Indefinite,
            }),
            5 => Some(match arg {
                Some(n) => Frame::Definite(n.saturating_mul(2)),
                None => Frame::Indefinite,
            }),
            // A tag is followed by exactly one item. Counting it as a level
            // is what the decoder does too: a Constr is a tag around an array.
            6 => Some(Frame::Definite(1)),
            // Simple values and floats. The argument was already consumed.
            _ => None,
        };

        match opened {
            Some(Frame::Definite(0)) => {
                // An empty container opens and closes in one step.
                deepest = deepest.max(stack.len() + 1);
                complete_item(&mut stack);
                if stack.is_empty() {
                    return Ok(deepest);
                }
            }
            Some(frame) => {
                stack.push(frame);
                deepest = deepest.max(stack.len());
                if deepest > max_depth {
                    return Err(DepthError::TooDeep(deepest));
                }
            }
            None => {
                deepest = deepest.max(stack.len());
                complete_item(&mut stack);
                if stack.is_empty() {
                    return Ok(deepest);
                }
            }
        }
    }
}

/// Record that one item finished, closing any definite containers it filled.
fn complete_item(stack: &mut Vec<Frame>) {
    while let Some(top) = stack.last_mut() {
        match top {
            Frame::Definite(remaining) => {
                *remaining -= 1;
                if *remaining == 0 {
                    stack.pop();
                } else {
                    return;
                }
            }
            // Stays open until its break byte.
            Frame::Indefinite => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `depth` nested indefinite arrays, each closed. This is the shape that
    /// aborts the process when it reaches the decoder.
    fn nested_indefinite(depth: usize) -> Vec<u8> {
        let mut v = vec![0x9f; depth];
        v.extend(std::iter::repeat(0xff).take(depth));
        v
    }

    fn nested_definite(depth: usize) -> Vec<u8> {
        // 0x81 is a one-item array; nesting them gives exactly `depth` levels.
        let mut v = vec![0x81; depth];
        v.push(0x00); // the innermost item
        v
    }

    #[test]
    fn a_flat_integer_has_no_depth() {
        assert_eq!(check_depth(&[0x00], MAX_CBOR_DEPTH), Ok(0));
    }

    #[test]
    fn a_wide_shallow_array_is_one_level() {
        // 1000 integers in one array. Width must not read as depth: a real
        // transcript is wide, and a limit that counted items would refuse it.
        let mut v = vec![0x99, 0x03, 0xe8];
        v.extend(std::iter::repeat(0x00).take(1000));
        assert_eq!(check_depth(&v, MAX_CBOR_DEPTH), Ok(1));
    }

    #[test]
    fn nesting_is_counted_for_both_encodings() {
        assert_eq!(check_depth(&nested_definite(10), MAX_CBOR_DEPTH), Ok(10));
        assert_eq!(check_depth(&nested_indefinite(10), MAX_CBOR_DEPTH), Ok(10));
    }

    #[test]
    fn the_stack_killing_input_is_refused() {
        // 4000 levels aborted the process when measured against the decoder.
        let v = nested_indefinite(4000);
        assert!(matches!(check_depth(&v, MAX_CBOR_DEPTH), Err(DepthError::TooDeep(_))));
    }

    #[test]
    fn refusal_happens_at_the_limit_not_at_the_end_of_input() {
        // The guard must stop as soon as the limit is passed, so a very large
        // body costs work proportional to the limit, not to its own size.
        let v = nested_indefinite(100_000);
        assert_eq!(check_depth(&v, 8), Err(DepthError::TooDeep(9)));
    }

    #[test]
    fn a_tag_counts_as_a_level() {
        // 0xd8 0x79 is tag 121, how a Constr is encoded, wrapping an array.
        let v = vec![0xd8, 0x79, 0x81, 0x00];
        assert_eq!(check_depth(&v, MAX_CBOR_DEPTH), Ok(2));
    }

    #[test]
    fn an_empty_container_opens_and_closes() {
        assert_eq!(check_depth(&[0x80], MAX_CBOR_DEPTH), Ok(1)); // []
        assert_eq!(check_depth(&[0xa0], MAX_CBOR_DEPTH), Ok(1)); // {}
    }

    #[test]
    fn a_map_holds_two_items_per_pair() {
        // {1: 2} — one pair, two items, one level.
        assert_eq!(check_depth(&[0xa1, 0x01, 0x02], MAX_CBOR_DEPTH), Ok(1));
    }

    #[test]
    fn a_byte_string_payload_is_skipped_not_walked() {
        // A 4-byte string whose contents look like array headers. Reading the
        // payload as structure would invent nesting that is not there.
        let v = vec![0x44, 0x9f, 0x9f, 0x9f, 0x9f];
        assert_eq!(check_depth(&v, MAX_CBOR_DEPTH), Ok(0));
    }

    #[test]
    fn a_truncated_item_is_reported_as_truncated() {
        assert_eq!(check_depth(&[0x44, 0x01], MAX_CBOR_DEPTH), Err(DepthError::Truncated));
        assert_eq!(check_depth(&[], MAX_CBOR_DEPTH), Err(DepthError::Truncated));
    }

    #[test]
    fn a_huge_declared_length_does_not_overflow_or_allocate() {
        // A byte string claiming 2^64-1 bytes. The guard must refuse, not
        // wrap the cursor round or try to hold the payload.
        let v = vec![0x5b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        assert_eq!(check_depth(&v, MAX_CBOR_DEPTH), Err(DepthError::Truncated));
    }

    #[test]
    fn the_scan_always_terminates_on_arbitrary_bytes() {
        // Every byte value as a one-byte input, then some random-ish noise.
        // The property under test is that the call returns at all.
        for b in 0u16..=255 {
            let _ = check_depth(&[b as u8], MAX_CBOR_DEPTH);
        }
        let noise: Vec<u8> = (0..4096u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
        let _ = check_depth(&noise, MAX_CBOR_DEPTH);
    }
}
