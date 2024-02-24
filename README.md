## PrestaShop 1.7 WebService client

Generically supports 65 out of 67 GET methods (image & search excluded)

Implemented in both https://github.com/apache/arrow-rs and https://github.com/jorgecarleitao/arrow2
for educational purposes

### Usage example
```rust
./target/debug/cli get-schema products --conf ./production.toml
./target/debug/cli get orders --limit 1000 \
    --date_upd 2020-01-02..2023-01-05 \
    --arrow2 \
    --output-format parquet \
    --output-path ./orders.parquet \
    --fields id \
    --fields total_paid \
    --field-value-in id=12,54,5 \
    --flatten1 \
    --conf ./production.toml
```
