# Gearman Worker Library for Rust

![Status: Alpha](https://img.shields.io/badge/status-alpha-red.svg?longCache=true "Status: Alpha")
[![Build Status](https://travis-ci.com/mtorromeo/gearman-worker-rs.svg?branch=master)](https://travis-ci.com/mtorromeo/gearman-worker-rs)

High level library to implement [Gearman] workers.

## Install

Add this dependency to your `Cargo.toml`

```toml
gearman-worker = "*"
```

## Usage

```rust
extern crate gearman_worker;

use gearman_worker::Worker;

fn main() {
    let server_addr = "127.0.0.1:4730".parse().unwrap();

    let mut worker = WorkerBuilder::new("my-worker-rs-1", server_addr).build();
    worker.connect().unwrap();

    worker.register_function("greet", |input| {
        let hello = String::from_utf8_lossy(input);
        let response = format!("{} world!", hello);
        Ok(response.into_bytes())
    }).unwrap();

    worker.run().unwrap();
}
```

where the worker functions have the following signature:
```rust
Fn(&[u8]) -> Result<Vec<u8>, Option<Vec<u8>>>;
```

## Known issues

This has not been tested yet with a real workload and the public interface will probably change in the future.

The worker runs in a single thread using blocking tcp connections. This is fine if you don't expect high concurrency and you can always spawn multiple separate processes to handle the workload but I plan on implementing multi-threading and non-blocking io (probably with tokio).

The following gearman operations are not currently supported but the typical use-case is implemented:

- WORK_STATUS
- CAN_DO_TIMEOUT
- WORK_DATA
- WORK_WARNING
- GRAB_JOB_UNIQ
- GRAB_JOB_ALL


## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) and [CONDUCT](CONDUCT.md) for details.

## Security

If you discover any security related issues, please email massimiliano.torromeo@gmail.com instead of using the issue tracker.

## Credits

- [Massimiliano Torromeo][link-author]
- [All Contributors][link-contributors]

## License

This software is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

[Gearman]: http://gearman.org/
[link-author]: https://github.com/mtorromeo
[link-contributors]: https://github.com/mtorromeo/gearman-worker-rs/graphs/contributors
