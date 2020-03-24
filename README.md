# protoc-gen-hive-schema

> Hive/Glue types: https://docs.aws.amazon.com/athena/latest/ug/data-types.html

## Setup
Requires:
* `protoc` (http://google.github.io/proto-lens/installing-protoc.html)

```sh
export PATH=$PWD/bin:$PATH
make install
```

## Usage

```sh
protoc --hive-schema_out=examples examples/foo.proto examples/bar.proto
```

---

### TODO
- [ ] Re-implement tests
