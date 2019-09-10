all:
	cargo build --release
getrust:
	curl https://sh.rustup.rs -sSf | sh -s -- -y
updaterust:
	rustup update stable
install:
	install -m 755 ./target/release/reredis /usr/local/bin
test:
	cargo test --test server_test -- --ignored --nocapture
