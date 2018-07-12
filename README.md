# Gearman Worker Library for Rust ![Status: Alpha](https://img.shields.io/badge/status-alpha-red.svg?longCache=true&style=flat-square "Status: Alpha")

High level library to implement [Gearman] workers.

## Install

Add this dependency to your `Cargo.toml`

```toml
gearman-worker-rs = "*"
```

## Usage

```rs
extern crate gearman_worker;

use gearman_worker::Worker;

fn main() {
    let server_addr = "127.0.0.1:4730".parse().unwrap();

    let mut worker = Worker::new(server_addr)
        .with_id("my-worker-rs-1")
        .connect().unwrap();

    worker.register_function("hello", |_| {
        Ok(b"world!")
    }).unwrap();

    worker.run().unwrap();
}
```

where the worker functions have the following signature:
```rs
Fn(&[u8]) -> Result<&[u8], gearman_worker::WorkError>;
```

## Known issues

This as not been tested yet with a real workload and the public interface will probably change in the future.

The worker runs in a single thread using blocking tcp connections. This is fine if you don't expect high concurrency and you can always spawn multiple separate processes to handle the workload but I plan on implementing multi-threading and non-blocking io (probably with tokio).

The following gearman operations are not currently supported but the typical use-case is implemented:

- WORK_STATUS
- CAN_DO_TIMEOUT
- WORK_EXCEPTION
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

The MIT License (MIT). Please see [License File](LICENSE) for more information.

[Gearman]: http://gearman.org/
[link-author]: https://github.com/mtorromeo
[link-contributors]: https://github.com/mtorromeo/gearman-worker-rs/graphs/contributors