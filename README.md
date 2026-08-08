# zplc

integer representation of did:plc identifiers.
it's basically just the order that they appear in the plc.directory export

## usage

```shell
$ # ingest from plc.directory exports:
$ cargo run --release -- ingest plc
$ # serve the zplc api:
$ cargo run --release -- serve
```

## API

```
GET /<zplc>        resolve a ZPLC number to a did:plc identifier
GET /did:plc:<id> resolve a did:plc identifier to a ZPLC number
GET /latest        creation timestamp of the latest ingested operation
```
