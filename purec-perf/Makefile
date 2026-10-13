BUILD ?= build
PREFIX ?= $(CURDIR)/install
.PHONY: all build check test install
all: build
build:
	cmake -S . -B $(BUILD) -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
	cmake --build $(BUILD) --parallel
check: build
test: build
	cd $(BUILD) && ctest --output-on-failure
install: build
	cmake --install $(BUILD) --prefix $(PREFIX)
