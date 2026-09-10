using System.Reflection;
using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class TypedZerobusStreamTests
{
    [Test]
    public void JsonStream_ExposesOnlyJsonIngestOverloads()
    {
        var singleRecord = typeof(JsonZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(JsonZerobusStream.IngestRecord))
            .ToArray();

        var batch = typeof(JsonZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(JsonZerobusStream.IngestRecords))
            .ToArray();

        Assert.That(singleRecord, Has.Length.EqualTo(1));
        Assert.That(singleRecord[0].GetParameters().Select(parameter => parameter.ParameterType),
            Is.EqualTo(new[] { typeof(string) }));

        Assert.That(batch, Has.Length.EqualTo(1));
        Assert.That(batch[0].GetParameters().Select(parameter => parameter.ParameterType),
            Is.EqualTo(new[] { typeof(string[]) }));
    }

    [Test]
    public void ProtoStream_ExposesOnlyProtoIngestOverloads()
    {
        var singleRecord = typeof(ProtoZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(ProtoZerobusStream.IngestRecord))
            .ToArray();

        var batch = typeof(ProtoZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(ProtoZerobusStream.IngestRecords))
            .ToArray();

        Assert.That(singleRecord.Select(method => method.GetParameters().Single().ParameterType),
            Is.EquivalentTo(new[] { typeof(byte[]), typeof(ReadOnlySpan<byte>) }));

        Assert.That(batch, Has.Length.EqualTo(1));
        Assert.That(batch[0].GetParameters().Select(parameter => parameter.ParameterType),
            Is.EqualTo(new[] { typeof(byte[][]) }));
    }

#if ZEROBUS_AVRO
    [Test]
    public void AvroStream_ExposesBytesAndObjectIngestOverloads()
    {
        var singleRecord = typeof(AvroZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(AvroZerobusStream.IngestRecord))
            .OrderBy(method => method.GetParameters().First().ParameterType.Name)
            .ToArray();

        var batch = typeof(AvroZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(AvroZerobusStream.IngestRecords))
            .OrderBy(method => method.GetParameters().First().ParameterType.Name)
            .ToArray();

        // Single record: byte[], ReadOnlySpan<byte>, object (ordered by name)
        Assert.That(singleRecord, Has.Length.EqualTo(3));
        var singleParamTypes = singleRecord.Select(m => m.GetParameters().Single().ParameterType).ToArray();
        Assert.That(singleParamTypes[0], Is.EqualTo(typeof(byte[])));
        Assert.That(singleParamTypes[1], Is.EqualTo(typeof(object)));
        Assert.That(singleParamTypes[2], Is.EqualTo(typeof(ReadOnlySpan<byte>)));

        // Batch: byte[][], object[]
        Assert.That(batch, Has.Length.EqualTo(2));
        var batchParamTypes = batch.Select(m => m.GetParameters().Single().ParameterType).ToArray();
        Assert.That(batchParamTypes[0], Is.EqualTo(typeof(byte[][])));
        Assert.That(batchParamTypes[1], Is.EqualTo(typeof(object[])));
    }
#endif
}