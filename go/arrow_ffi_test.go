package zerobus

import "testing"

func TestArrowConfigIPCCompressionDefaultsToZstd(t *testing.T) {
	cases := []struct {
		name string
		opts *ArrowStreamConfigurationOptions
		want int32
	}{
		{"nil options", nil, 1},
		{"default options", DefaultArrowStreamConfigurationOptions(), 1},
		{"zero value", &ArrowStreamConfigurationOptions{}, 1},
		{"explicit zstd", &ArrowStreamConfigurationOptions{IPCCompression: IPCCompressionZstd}, 1},
		{"lz4 frame", &ArrowStreamConfigurationOptions{IPCCompression: IPCCompressionLZ4Frame}, 0},
		{"none", &ArrowStreamConfigurationOptions{IPCCompression: IPCCompressionNone}, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := int32(convertArrowConfigToC(tc.opts).ipc_compression); got != tc.want {
				t.Errorf("ipc_compression = %d, want %d", got, tc.want)
			}
		})
	}
}
