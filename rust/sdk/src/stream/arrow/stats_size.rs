//! Size accounting from the IPC buffers already produced by the encoder.

use arrow_flight::FlightData;

/// Returns (record rows, uncompressed buffer bytes). Dictionary frames contribute
/// bytes but no rows. Reading IPC lengths handles every array layout without
/// walking arrays, copying their payloads, or decompressing/re-encoding anything.
pub(super) fn frame_stats(frame: &FlightData) -> Option<(u64, u64)> {
    let message = arrow_ipc::root_as_message(&frame.data_header).ok()?;
    let (rows, batch) = if let Some(batch) = message.header_as_record_batch() {
        (u64::try_from(batch.length()).ok()?, batch)
    } else {
        (0, message.header_as_dictionary_batch()?.data()?)
    };
    let bytes = batch.buffers()?.iter().try_fold(0_u64, |total, buffer| {
        let length = u64::try_from(buffer.length()).ok()?;
        let uncompressed = if batch.compression().is_none() || length == 0 {
            length
        } else {
            // Compressed IPC buffers start with a little-endian i64 containing
            // their original length. -1 means the remaining bytes are stored raw.
            let offset = usize::try_from(buffer.offset()).ok()?;
            let prefix = frame.data_body.get(offset..)?.get(..8)?;
            match i64::from_le_bytes(prefix.try_into().ok()?) {
                -1 => length.checked_sub(8)?,
                value => u64::try_from(value).ok()?,
            }
        };
        total.checked_add(uncompressed)
    })?;
    Some((rows, bytes))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::types::{Int32Type, Int8Type};
    use arrow_array::{
        Array, ArrayRef, BinaryViewArray, DictionaryArray, Int8Array, LargeListArray, ListArray,
        RecordBatch, StringArray, StringViewArray, StructArray,
    };
    use arrow_flight::encode::{DictionaryHandling, FlightDataEncoderBuilder};
    use arrow_ipc::CompressionType;
    use arrow_schema::Field;
    use futures::StreamExt;

    use super::*;

    async fn encode_stats(array: ArrayRef, codec: Option<CompressionType>) -> (u64, u64, usize) {
        let batch = RecordBatch::try_from_iter([("value", array)]).unwrap();
        let mut frames = FlightDataEncoderBuilder::new()
            .with_dictionary_handling(DictionaryHandling::Resend)
            .with_options(super::super::batch::make_ipc_write_options(codec).unwrap())
            .build(futures::stream::iter([Ok(batch)]));
        let mut totals = (0, 0, 0);
        while let Some(frame) = frames.next().await {
            if let Some((rows, bytes)) = frame_stats(&frame.unwrap()) {
                totals.0 += rows;
                totals.1 += bytes;
                totals.2 += 1;
            }
        }
        totals
    }

    #[tokio::test]
    async fn counts_encoded_buffers_for_sliced_nested_arrays_and_views() {
        let values = || (0..10_000).map(|i| Some(vec![Some(i)]));
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(values()));
        let large: ArrayRef = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
            values(),
        ));
        let structure = StructArray::from(vec![(
            Arc::new(Field::new("items", lists.data_type().clone(), false)),
            lists.clone(),
        )]);
        let unused = "y".repeat(20_000);
        let long = "x".repeat(1_000);
        let strings = StringViewArray::from(vec![unused.as_str(), long.as_str(), "inline"]);
        let binary = BinaryViewArray::from(vec![unused.as_bytes(), long.as_bytes()]);
        // IPC emits validity bitmaps and slices list children. For views, Arrow
        // currently emits the full external buffers, even after slicing rows.
        let arrays: Vec<(ArrayRef, u64)> = vec![
            (lists.slice(9_999, 1), 1 + 8 + 1 + 4),
            (large.slice(9_999, 1), 1 + 16 + 1 + 4),
            (Arc::new(structure.slice(9_999, 1)), 1 + 1 + 8 + 1 + 4),
            (Arc::new(strings.slice(1, 1)), 1 + 16 + 21_000),
            (Arc::new(binary.slice(1, 1)), 1 + 16 + 21_000),
        ];
        for (array, expected) in arrays {
            for codec in [
                None,
                Some(CompressionType::LZ4_FRAME),
                Some(CompressionType::ZSTD),
            ] {
                assert_eq!(encode_stats(array.clone(), codec).await, (1, expected, 1));
            }
        }
    }

    #[tokio::test]
    async fn counts_dictionary_buffers_without_counting_dictionary_rows() {
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(1)]),
                Arc::new(StringViewArray::from(vec![
                    "x".repeat(1_000),
                    "inline".to_string(),
                ])),
            )
            .unwrap(),
        );
        for codec in [
            None,
            Some(CompressionType::LZ4_FRAME),
            Some(CompressionType::ZSTD),
        ] {
            // Dictionary: validity + two views + payload; batch: validity + keys.
            assert_eq!(
                encode_stats(dictionary.clone(), codec).await,
                (3, 1 + 32 + 1_000 + 1 + 3, 2)
            );
            // Empty buffers have no compression length prefix.
            assert_eq!(
                encode_stats(Arc::new(StringArray::from(vec![""])), codec).await,
                (1, 1 + 8, 1)
            );
        }
    }
}
