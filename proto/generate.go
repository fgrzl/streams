//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
//go:generate go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

//go:generate protoc --proto_path=./ --go_out=../pkg/models --go_opt=paths=source_relative --go_opt=Mmodels.proto=github.com/fgrzl/streams/pkg/models ./models.proto
//go:generate protoc --proto_path=./ --go-grpc_out=../pkg/grpcservices --go-grpc_opt=paths=source_relative,Mservices.proto=github.com/fgrzl/streams/pkg/grpcservices,Mmodels.proto=github.com/fgrzl/streams/pkg/models ./services.proto
//go:generate protoc --proto_path=./ --go_out=../pkg/grpcservices --go_opt=paths=source_relative,Mservices.proto=github.com/fgrzl/streams/pkg/grpcservices,Mmodels.proto=github.com/fgrzl/streams/pkg/models ./services.proto

package proto
