#if ZEROBUS_AVRO

using System.IO;
using Avro;
using Avro.Generic;

namespace Databricks.Zerobus;

/// <summary>
/// A stream that accepts Avro record objects or pre-encoded binary payloads (Beta).
/// Encodes objects natively using Apache.Avro schema parsing and binary encoding.
/// </summary>
public sealed class AvroZerobusStream : TypedZerobusStream
{
    private readonly Schema _schema;

    internal AvroZerobusStream(ZerobusStream innerStream, string avroSchemaJson)
        : base(innerStream)
    {
        _schema = Schema.Parse(avroSchemaJson);
    }

    /// <summary>
    /// Encodes a record object to Avro binary format.
    /// </summary>
    private byte[] EncodeRecord(object value)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));

        var record = ObjectToGenericRecord(value);
        using var ms = new MemoryStream();
        var encoder = new Avro.IO.BinaryEncoder(ms);
        var writer = new GenericDatumWriter<GenericRecord>(_schema);
        writer.Write(record, encoder);
        return ms.ToArray();
    }

    /// <summary>
    /// Converts a CLR object to a GenericRecord for encoding.
    /// Handles primitive types, complex types, and Apache.Avro wrapped types.
    /// </summary>
    private GenericRecord ObjectToGenericRecord(object value)
    {
        if (value is GenericRecord gr)
            return gr;

        if (value is not System.Collections.Generic.IDictionary<string, object> dict)
            throw new ArgumentException($"Expected dict-like object or GenericRecord, got {value.GetType().Name}");

        if (_schema is not RecordSchema recordSchema)
            throw new InvalidOperationException("Stream schema is not a record type");

        var record = new GenericRecord(recordSchema);
        foreach (var field in recordSchema.Fields)
        {
            if (!dict.TryGetValue(field.Name, out var val))
                continue;

            record.Add(field.Name, val);
        }

        return record;
    }

    /// <summary>
    /// Ingests a pre-encoded Avro binary record and returns the offset.
    /// </summary>
    public long IngestRecord(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return InnerStream.IngestRecord(payload);
    }

    /// <summary>
    /// Ingests a pre-encoded Avro binary record asynchronously and returns the offset.
    /// </summary>
    public Task<long> IngestRecordAsync(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return InnerStream.IngestRecordAsync(payload);
    }

    /// <summary>
    /// Ingests a pre-encoded Avro binary record and returns the offset.
    /// </summary>
    public long IngestRecord(ReadOnlySpan<byte> payload)
    {
        return InnerStream.IngestRecord(payload);
    }

    /// <summary>
    /// Ingests an Avro record object, encoding it natively against the stream's schema.
    /// </summary>
    /// <param name="value">The record object to ingest (dict-like or GenericRecord).</param>
    /// <returns>The offset of the ingested record.</returns>
    public long IngestRecord(object value)
    {
        var encoded = EncodeRecord(value);
        return InnerStream.IngestRecord(encoded);
    }

    /// <summary>
    /// Ingests an Avro record object asynchronously, encoding it natively.
    /// </summary>
    /// <param name="value">The record object to ingest.</param>
    /// <returns>A task that resolves to the offset of the ingested record.</returns>
    public Task<long> IngestRecordAsync(object value)
    {
        var encoded = EncodeRecord(value);
        return InnerStream.IngestRecordAsync(encoded);
    }

    /// <summary>
    /// Ingests a batch of pre-encoded Avro binary records and returns the batch offset.
    /// </summary>
    public long IngestRecords(byte[][] records)
    {
        ArgumentNullException.ThrowIfNull(records);
        return InnerStream.IngestRecords(records);
    }

    /// <summary>
    /// Ingests a batch of pre-encoded Avro binary records asynchronously and returns the batch offset.
    /// </summary>
    public Task<long> IngestRecordsAsync(byte[][] records)
    {
        ArgumentNullException.ThrowIfNull(records);
        return InnerStream.IngestRecordsAsync(records);
    }

    /// <summary>
    /// Ingests a batch of Avro record objects, encoding them natively.
    /// </summary>
    /// <param name="values">The record objects to ingest.</param>
    /// <returns>The batch offset of the ingested records.</returns>
    public long IngestRecords(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        if (values.Length == 0)
            return -1;

        var encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
            encoded[i] = EncodeRecord(values[i]);

        return InnerStream.IngestRecords(encoded);
    }

    /// <summary>
    /// Ingests a batch of Avro record objects asynchronously, encoding them natively.
    /// </summary>
    /// <param name="values">The record objects to ingest.</param>
    /// <returns>A task that resolves to the batch offset of the ingested records.</returns>
    public Task<long> IngestRecordsAsync(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        if (values.Length == 0)
            return Task.FromResult(-1L);

        var encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
            encoded[i] = EncodeRecord(values[i]);

        return InnerStream.IngestRecordsAsync(encoded);
    }

    /// <summary>
    /// Retrieves all records that have not yet been acknowledged by the server.
    /// <para>
    /// <strong>Important:</strong> This should only be called after the stream has
    /// closed or failed. Calling it on an active stream will return an error.
    /// </para>
    /// </summary>
    /// <returns>
    /// An array of raw Avro record payloads as <see cref="ReadOnlyMemory{T}"/> of <see cref="byte"/>.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if retrieval fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public ReadOnlyMemory<byte>[] GetUnackedRecords()
    {
        return InnerStream.GetUnackedRecords();
    }

    /// <summary>
    /// Asynchronously retrieves all records that have not yet been acknowledged by the server.
    /// <para>
    /// <strong>Important:</strong> This should only be called after the stream has
    /// closed or failed. Calling it on an active stream will return an error.
    /// </para>
    /// </summary>
    /// <returns>
    /// A task that resolves to an array of raw Avro record payloads as <see cref="ReadOnlyMemory{T}"/> of <see cref="byte"/>.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if retrieval fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public Task<ReadOnlyMemory<byte>[]> GetUnackedRecordsAsync()
    {
        return InnerStream.GetUnackedRecordsAsync();
    }
}

#endif
