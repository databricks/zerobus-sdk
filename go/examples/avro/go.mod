module avro-example

go 1.24.0

require github.com/databricks/zerobus-sdk/go v0.0.0

require (
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/hamba/avro/v2 v2.31.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
)

replace github.com/databricks/zerobus-sdk/go => ../..
