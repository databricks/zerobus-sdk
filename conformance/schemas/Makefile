# Regenerates the compiled protobuf descriptor sets for every shared schema.
#
#   make -C conformance/schemas desc
#
# For each <name>/schema.proto this writes <name>/build/<name>.desc, a serialized
# FileDescriptorSet (imports included, no source info) that the orchestrator
# reads to ship the DescriptorProto bytes in CreateStream.
#
# The .desc files are committed. Re-run this target after editing any schema.proto
# and commit the result together with the .proto change.

PROTOC ?= protoc
SCHEMA_DIRS := $(patsubst %/,%,$(sort $(dir $(wildcard */schema.proto))))

.PHONY: desc clean-desc

desc:
	@test -n "$(SCHEMA_DIRS)" || { echo "no */schema.proto found"; exit 1; }
	@for name in $(SCHEMA_DIRS); do \
		mkdir -p $$name/build && \
		$(PROTOC) -I $$name --include_imports \
			--descriptor_set_out=$$name/build/$$name.desc \
			$$name/schema.proto && \
		echo "wrote $$name/build/$$name.desc"; \
	done

clean-desc:
	rm -f $(foreach n,$(SCHEMA_DIRS),$(n)/build/$(n).desc)
