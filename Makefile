.PHONY: clean

BIN := bin
GEN := gen
MAIN := cmd/main.go
DEPS := $(MAIN) gen/annales.pb.go gen/annales_grpc.pb.go
SCHEMAS := https://raw.githubusercontent.com/spoletum/schemas/main

$(BIN)/annales: $(DEPS)
	go build -o $@ $(MAIN)

$(GEN):
	mkdir -p $@

$(GEN)/%.proto: $(GEN)
	curl -so $@ $(SCHEMAS)/$*.proto

$(GEN)/%.pb.go: $(GEN)/%.proto
	protoc -I$(GEN) --go_out=paths=source_relative:$(GEN) $<

$(GEN)/%_grpc.pb.go: $(GEN)/%.proto
	protoc -I$(GEN) --go-grpc_out=paths=source_relative:$(GEN) $<

clean:
	rm -Rf $(BIN) $(GEN)