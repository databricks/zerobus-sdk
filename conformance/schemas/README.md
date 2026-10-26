# Conformance shared schemas

Language-agnostic record schemas referenced by name from conformance scenarios.
See `conformance/DESIGN.md` §6 and `conformance/CONTRACTS.md` for the design and the
interfaces this directory conforms to.

## Layout

```
conformance/schemas/<name>/
  schema.proto            # authored: protobuf definition
  schema.avsc             # authored: Avro record equivalent
  schema.arrow.json       # authored, only if a scenario needs Arrow (not yet present)
  build/<name>.desc       # generated, committed: serialized FileDescriptorSet
  golden/<fmt>/<sha1>.bin # generated, committed: frozen expected wire bytes
  golden/MANIFEST.json    # generated, committed: record, file and hex for each .bin
```

The orchestrator loads a schema by name, reads `build/<name>.desc` for the
`DescriptorProto` bytes it ships in `CreateStream`, and reads `schema.avsc` for Avro
encoding. Golden bytes are frozen and committed. The orchestrator reads them and never
encodes at assert time. They are generated once by
`conformance/orchestrator/scripts/regen_goldens.py` (independent protobuf and fastavro
encoders) and reviewed by a human before commit.

Keying: `<sha1>` is the SHA-1 of the canonical JSON of the logical record (sorted keys,
compact separators, UTF-8). `<fmt>` is the directory. `golden/MANIFEST.json` maps each
file back to its record and shows its hex. To refresh after a schema or record change:

```sh
conformance/orchestrator/.venv/bin/python conformance/orchestrator/scripts/regen_goldens.py
```

Review the diff (the MANIFEST `hex` field is the quick check) and commit it. If a required
`.bin` is missing, the scenario fails validation and names the file and this command.

## Schemas

### `simple_record`

Package `zerobus.conformance.fixtures`.

| Field     | Proto (`schema.proto`) | Avro (`schema.avsc`) | Notes                 |
|-----------|------------------------|----------------------|-----------------------|
| `id`      | `int64 id = 1`         | `long`               | JSON number within 2^53 |
| `message` | `string message = 2`   | `string`             | UTF-8                 |

Example logical record (`record_json`): `{"id": 1, "message": "hello"}`.

## Common-core type set

A schema's supported formats follow from the types it uses. The common core is
expressible in every format (proto, json, avro):

- `bool`, `int64`, `double`, `string`, `bytes`, `struct`, `list`

Format-exclusive types restrict a schema to the formats that can express them. Examples
are Avro `union`, `fixed`, and `decimal`. `simple_record` uses only `int64` and `string`,
so it is valid for all formats. Arrow is excluded from byte-exact scenarios, so no
Arrow-specific schema exists yet.

JSON canonical conventions: int64 as a JSON number within 2^53 (a string beyond that),
bytes as base64.

## Regenerating goldens

See "Layout" above. `regen_goldens.py` records every scenario, collects each `golden(...)`
used in an expectation, writes the matching `.bin` files, and deletes `.bin` files that no
scenario references any more.

## Regenerating descriptors

Requires `protoc` on `PATH`, or set `PROTOC=/path/to/protoc`.

```sh
make -C conformance/schemas desc          # rebuild every <name>/build/<name>.desc
make -C conformance/schemas clean-desc    # remove generated .desc files
```

After editing any `schema.proto`, re-run `make desc` and commit the regenerated `.desc`
with the `.proto` change. The Makefile compiles with `--include_imports` and no source
info, so the output contains no comments or paths beyond the file name.

Note: the `.desc` embeds the file name as `schema.proto` (the path relative to the include
directory), not `<name>/schema.proto`.

To check a `.desc` by hand (requires the `protobuf` Python package):

```python
from google.protobuf import descriptor_pb2
fds = descriptor_pb2.FileDescriptorSet()
fds.ParseFromString(open("conformance/schemas/simple_record/build/simple_record.desc", "rb").read())
for f in fds.file:
    for m in f.message_type:
        print(m.name, [(x.name, x.number) for x in m.field])
```
