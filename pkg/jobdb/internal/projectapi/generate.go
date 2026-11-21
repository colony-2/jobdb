package projectapi

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.5.0 -package projectapi -include-tags projects -generate types,client -response-type-suffix HTTPResponse -o zz_generated.go ../../../../openapi/jobdb-runtime.yaml
