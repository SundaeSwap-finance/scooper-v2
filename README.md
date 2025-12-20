Example sync point:

preview:
```sh
cargo run -- --protocol config/protocols/preview.json sync-from-point --block-hash 7bfb6a677df577d2f0371236ecf63554b54b35b663d3ad9159695a609306e629 --slot 48460699
```

mainnet:
```sh
cargo run -- --protocol config/protocols/mainnet.json sync-from-point --block-hash 5acee019d5550554aff5a044ed3b700decf29460e100218381f85a767af8c09f --slot 123665404
```