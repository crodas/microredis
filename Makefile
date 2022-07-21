fmt:
	cargo fmt
clippy:
	cargo clippy --release
build:
	cargo build --release
test: build
	./runtest  --clients 5 \
		--skipunit unit/dump \
		--skipunit unit/auth \
		--skipunit unit/protocol \
		--skipunit unit/scan \
		--skipunit unit/info \
		--skipunit unit/type/zset \
		--skipunit unit/bitops \
		--skipunit unit/type/stream \
		--skipunit unit/type/stream-cgroups \
		--skipunit unit/sort \
		--skipunit unit/aofrw \
		--skipunit unit/acl \
		--skipunit unit/latency-monitor \
		--skipunit unit/slowlog \
		--skipunit unit/scripting \
		--skipunit unit/introspection \
		--skipunit unit/introspection-2 \
		--skipunit unit/bitfield \
		--skipunit unit/geo \
		--skipunit unit/pause \
		--skipunit unit/hyperloglog \
		--skipunit unit/lazyfree \
		--skipunit unit/tracking \
		--skipunit unit/querybuf \
		--ignore-encoding \
		--tags -needs:repl \
		--tags -leaks \
		--tags -needs:debug \
		--tags -needs:save \
		--tags -external:skip \
		--tags -needs:save \
		--tags -consistency \
		--tags -cli \
		--tags -needs:config-maxmemory
ci: fmt clippy build test
