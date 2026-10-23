// Logical record -> SDK-native record conversion. The only place this harness
// understands record bodies.
//
// Pipeline: `CreateStreamRequest.descriptor_proto_bytes` -> ProtoRecordEncoder (parsed once
// per stream); `IngestRecordRequest.record_json` -> ProtoRecordEncoder.Encode -> protobuf wire
// bytes, which the SDK's `IngestRecord(byte[])` ships as-is. JSON streams pass the validated
// object string straight to the SDK, which encodes it itself.
//
// Scope: proto3 scalars, repeated scalars, nested messages and enums declared inside the same
// DescriptorProto. Map fields and cross-file type references are rejected with
// ArgumentException, so a schema the harness cannot encode fails loudly.

using System.Globalization;
using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using FieldType = Google.Protobuf.Reflection.FieldDescriptorProto.Types.Type;
using FieldLabel = Google.Protobuf.Reflection.FieldDescriptorProto.Types.Label;
using WireType = Google.Protobuf.WireFormat.WireType;

namespace Zerobus.Conformance.Harness;

/// <summary>Encodes canonical protobuf-JSON records against one serialized DescriptorProto.</summary>
internal sealed class ProtoRecordEncoder
{
    private readonly DescriptorProto _message;

    private ProtoRecordEncoder(DescriptorProto message)
    {
        _message = message;
    }

    /// <summary>Parses a serialized <c>google.protobuf.DescriptorProto</c>.</summary>
    /// <exception cref="ArgumentException">The bytes are not a DescriptorProto with a name.</exception>
    public static ProtoRecordEncoder FromDescriptorProto(byte[] bytes)
    {
        DescriptorProto message;
        try
        {
            message = DescriptorProto.Parser.ParseFrom(bytes);
        }
        catch (InvalidProtocolBufferException e)
        {
            throw new ArgumentException($"descriptor_proto_bytes is not a DescriptorProto: {e.Message}", e);
        }

        if (message.Name.Length == 0)
        {
            throw new ArgumentException("descriptor_proto_bytes has no message name");
        }

        return new ProtoRecordEncoder(message);
    }

    /// <summary>Encodes one logical record (a JSON object) to protobuf wire bytes.</summary>
    /// <exception cref="ArgumentException">The record does not match the schema.</exception>
    public byte[] Encode(string recordJson)
    {
        using var doc = ParseObject(recordJson);
        return EncodeMessage(_message, doc.RootElement);
    }

    private static byte[] EncodeMessage(DescriptorProto message, JsonElement obj)
    {
        var fieldsByName = IndexFields(message);

        // Collect the present fields first so they can be written in field-number order,
        // which is the order canonical protobuf encoders emit.
        var present = new List<(FieldDescriptorProto Field, JsonElement Value)>();
        foreach (var property in obj.EnumerateObject())
        {
            if (!fieldsByName.TryGetValue(property.Name, out var field))
            {
                throw new ArgumentException(
                    $"record_json has unknown field '{property.Name}' for message '{message.Name}'");
            }

            if (property.Value.ValueKind == JsonValueKind.Null)
            {
                continue; // protobuf-JSON treats null as "not set"
            }

            present.Add((field, property.Value));
        }

        present.Sort((a, b) => a.Field.Number.CompareTo(b.Field.Number));

        using var buffer = new MemoryStream();
        var output = new CodedOutputStream(buffer);
        foreach (var (field, value) in present)
        {
            WriteField(output, message, field, value);
        }

        output.Flush();
        return buffer.ToArray();
    }

    private static void WriteField(CodedOutputStream output, DescriptorProto message, FieldDescriptorProto field, JsonElement value)
    {
        var type = field.Type;
        var repeated = field.Label == FieldLabel.Repeated;

        if (repeated)
        {
            if (value.ValueKind != JsonValueKind.Array)
            {
                throw new ArgumentException($"field '{field.Name}' is repeated and must be a JSON array");
            }

            if (IsPackable(type))
            {
                // proto3 packs repeated scalars by default: one length-delimited run.
                using var packed = new MemoryStream();
                var packedOut = new CodedOutputStream(packed);
                foreach (var element in value.EnumerateArray())
                {
                    WriteScalarValue(packedOut, message, field, element);
                }

                packedOut.Flush();
                output.WriteTag(field.Number, WireType.LengthDelimited);
                output.WriteBytes(ByteString.CopyFrom(packed.ToArray()));
                return;
            }

            foreach (var element in value.EnumerateArray())
            {
                WriteOne(output, message, field, element);
            }

            return;
        }

        // Singular proto3 field. Implicit-presence scalars holding their default value are
        // not written (matches canonical encoders). Messages and proto3 optionals always are.
        if (IsScalarOrEnum(type) && !field.Proto3Optional && IsDefaultScalar(message, field, value))
        {
            return;
        }

        WriteOne(output, message, field, value);
    }

    private static void WriteOne(CodedOutputStream output, DescriptorProto message, FieldDescriptorProto field, JsonElement value)
    {
        output.WriteTag(field.Number, WireTypeOf(message, field));
        if (field.Type == FieldType.Message)
        {
            var nested = ResolveNested(message, field);
            if (value.ValueKind != JsonValueKind.Object)
            {
                throw new ArgumentException($"field '{field.Name}' is a message and must be a JSON object");
            }

            output.WriteBytes(ByteString.CopyFrom(EncodeMessage(nested, value)));
            return;
        }

        WriteScalarValue(output, message, field, value);
    }

    /// <summary>Writes a scalar or enum value without its tag.</summary>
    private static void WriteScalarValue(CodedOutputStream o, DescriptorProto message, FieldDescriptorProto field, JsonElement value)
    {
        switch (field.Type)
        {
            case FieldType.Double:
                o.WriteDouble(ReadDouble(field, value));
                break;
            case FieldType.Float:
                o.WriteFloat((float)ReadDouble(field, value));
                break;
            case FieldType.Int64:
            case FieldType.Sfixed64:
            case FieldType.Sint64:
                {
                    var v = ReadInt64(field, value);
                    if (field.Type == FieldType.Int64) o.WriteInt64(v);
                    else if (field.Type == FieldType.Sint64) o.WriteSInt64(v);
                    else o.WriteSFixed64(v);
                    break;
                }
            case FieldType.Uint64:
            case FieldType.Fixed64:
                {
                    var v = ReadUInt64(field, value);
                    if (field.Type == FieldType.Uint64) o.WriteUInt64(v);
                    else o.WriteFixed64(v);
                    break;
                }
            case FieldType.Int32:
            case FieldType.Sint32:
            case FieldType.Sfixed32:
                {
                    var v = checked((int)ReadInt64(field, value));
                    if (field.Type == FieldType.Int32) o.WriteInt32(v);
                    else if (field.Type == FieldType.Sint32) o.WriteSInt32(v);
                    else o.WriteSFixed32(v);
                    break;
                }
            case FieldType.Uint32:
            case FieldType.Fixed32:
                {
                    var v = checked((uint)ReadUInt64(field, value));
                    if (field.Type == FieldType.Uint32) o.WriteUInt32(v);
                    else o.WriteFixed32(v);
                    break;
                }
            case FieldType.Bool:
                if (value.ValueKind != JsonValueKind.True && value.ValueKind != JsonValueKind.False)
                {
                    throw new ArgumentException($"field '{field.Name}' must be a JSON boolean");
                }

                o.WriteBool(value.GetBoolean());
                break;
            case FieldType.String:
                if (value.ValueKind != JsonValueKind.String)
                {
                    throw new ArgumentException($"field '{field.Name}' must be a JSON string");
                }

                o.WriteString(value.GetString()!);
                break;
            case FieldType.Bytes:
                {
                    if (value.ValueKind != JsonValueKind.String)
                    {
                        throw new ArgumentException($"field '{field.Name}' must be a base64 JSON string");
                    }

                    byte[] raw;
                    try
                    {
                        raw = Convert.FromBase64String(value.GetString()!);
                    }
                    catch (FormatException e)
                    {
                        throw new ArgumentException($"field '{field.Name}' is not valid base64: {e.Message}", e);
                    }

                    o.WriteBytes(ByteString.CopyFrom(raw));
                    break;
                }
            case FieldType.Enum:
                o.WriteInt32(ReadEnumNumber(message, field, value));
                break;
            default:
                throw new ArgumentException($"field '{field.Name}' has unsupported type {field.Type}");
        }
    }

    private static WireType WireTypeOf(DescriptorProto message, FieldDescriptorProto field) => field.Type switch
    {
        FieldType.Double or FieldType.Fixed64 or FieldType.Sfixed64 => WireType.Fixed64,
        FieldType.Float or FieldType.Fixed32 or FieldType.Sfixed32 => WireType.Fixed32,
        FieldType.String or FieldType.Bytes or FieldType.Message => WireType.LengthDelimited,
        _ => WireType.Varint,
    };

    private static bool IsScalarOrEnum(FieldType type) =>
        type is not (FieldType.Message or FieldType.Group);

    private static bool IsPackable(FieldType type) =>
        IsScalarOrEnum(type) && type is not (FieldType.String or FieldType.Bytes);

    /// <summary>True when a scalar holds its proto3 default value (omitted on the wire).</summary>
    private static bool IsDefaultScalar(DescriptorProto message, FieldDescriptorProto field, JsonElement value) => field.Type switch
    {
        FieldType.Double or FieldType.Float => ReadDouble(field, value) == 0,
        FieldType.Bool => value.ValueKind == JsonValueKind.False,
        FieldType.String or FieldType.Bytes => value.ValueKind == JsonValueKind.String && value.GetString()!.Length == 0,
        FieldType.Enum => ReadEnumNumber(message, field, value) == 0,
        FieldType.Uint64 or FieldType.Fixed64 or FieldType.Uint32 or FieldType.Fixed32 => ReadUInt64(field, value) == 0,
        _ => ReadInt64(field, value) == 0,
    };

    private static Dictionary<string, FieldDescriptorProto> IndexFields(DescriptorProto message)
    {
        var index = new Dictionary<string, FieldDescriptorProto>(StringComparer.Ordinal);
        foreach (var field in message.Field)
        {
            index[field.Name] = field;
            // protobuf-JSON also accepts the lowerCamelCase json_name.
            var jsonName = field.JsonName.Length > 0 ? field.JsonName : ToLowerCamel(field.Name);
            index.TryAdd(jsonName, field);
        }

        return index;
    }

    private static string ToLowerCamel(string snake)
    {
        var parts = snake.Split('_');
        return parts[0] + string.Concat(parts.Skip(1).Select(p => p.Length == 0 ? p : char.ToUpperInvariant(p[0]) + p[1..]));
    }

    private static DescriptorProto ResolveNested(DescriptorProto message, FieldDescriptorProto field)
    {
        var simple = LastSegment(field.TypeName);
        var nested = message.NestedType.FirstOrDefault(n => n.Name == simple);
        return nested ?? throw new ArgumentException(
            $"field '{field.Name}' references message '{field.TypeName}', which is not nested in '{message.Name}' (unsupported)");
    }

    private static int ReadEnumNumber(DescriptorProto message, FieldDescriptorProto field, JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Number)
        {
            return value.GetInt32();
        }

        if (value.ValueKind != JsonValueKind.String)
        {
            throw new ArgumentException($"enum field '{field.Name}' must be a name or number");
        }

        var simple = LastSegment(field.TypeName);
        var enumType = message.EnumType.FirstOrDefault(e => e.Name == simple)
            ?? throw new ArgumentException(
                $"enum field '{field.Name}' references '{field.TypeName}', which is not nested in '{message.Name}' (unsupported)");
        var name = value.GetString();
        var entry = enumType.Value.FirstOrDefault(v => v.Name == name)
            ?? throw new ArgumentException($"'{name}' is not a value of enum '{enumType.Name}'");
        return entry.Number;
    }

    private static string LastSegment(string typeName) =>
        typeName[(typeName.LastIndexOf('.') + 1)..];

    private static long ReadInt64(FieldDescriptorProto field, JsonElement value)
    {
        // Canonical protobuf-JSON: a JSON number within 2^53, or a decimal string beyond that.
        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var n)) return n;
        if (value.ValueKind == JsonValueKind.String &&
            long.TryParse(value.GetString(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var s)) return s;
        throw new ArgumentException($"field '{field.Name}' must be an integer");
    }

    private static ulong ReadUInt64(FieldDescriptorProto field, JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Number && value.TryGetUInt64(out var n)) return n;
        if (value.ValueKind == JsonValueKind.String &&
            ulong.TryParse(value.GetString(), NumberStyles.None, CultureInfo.InvariantCulture, out var s)) return s;
        throw new ArgumentException($"field '{field.Name}' must be a non-negative integer");
    }

    private static double ReadDouble(FieldDescriptorProto field, JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Number) return value.GetDouble();
        if (value.ValueKind == JsonValueKind.String)
        {
            // protobuf-JSON spells the non-finite values as strings.
            switch (value.GetString())
            {
                case "NaN": return double.NaN;
                case "Infinity": return double.PositiveInfinity;
                case "-Infinity": return double.NegativeInfinity;
                case var s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d): return d;
            }
        }

        throw new ArgumentException($"field '{field.Name}' must be a number");
    }

    private static JsonDocument ParseObject(string recordJson)
    {
        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(recordJson);
        }
        catch (JsonException e)
        {
            throw new ArgumentException($"record_json is not valid JSON: {e.Message}", e);
        }

        if (doc.RootElement.ValueKind != JsonValueKind.Object)
        {
            doc.Dispose();
            throw new ArgumentException("record_json must be a JSON object");
        }

        return doc;
    }

    /// <summary>
    /// Validates a JSON-stream record. The SDK encodes JSON itself, so the string is
    /// passed through unchanged once it is known to be a JSON object.
    /// </summary>
    public static void RequireJsonObject(string recordJson)
    {
        using var doc = ParseObject(recordJson);
    }
}
