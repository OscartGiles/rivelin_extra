## 🐦‍⬛ Overview

Welcome to the GitHub repository for rivelin_extra, a collection of open source crates developed for the Rivelin Project. The Rivelin Project is a cloud container building service written in Rust. It's currently under development and primarily exists as a learning project and maybe to help me get a job.

The main project lives at [https://github.com/OscartGiles/rivelin](https://github.com/OscartGiles/rivelin). If you are looking at my CV give me a message on [linkedin](https://www.linkedin.com/in/oscart-giles/) and I'll be happy to grant you read access.

### 📦 Crates

The following crates are included in this repository:

- [rivelin_actors](rivelin_actors): A tokio based actor framework. Includes some actor implementations including a pub/sub event bus.

- [rivelin_packit](rivelin_packit): Coming soon... A WASM based OCI compliant image builder. Inspired by [nixpacks](https://nixpacks.com/docs/getting-started) and [buildpacks](https://buildpacks.io/).

### Developing

To run all tests cd into a crate and run:

```sh
cargo test
```

📖 To build the docs run:

```sh
cargo doc --open
```

## ⚖️ License

Licensed under the MIT license. See [LICENSE](./LICENSE).
