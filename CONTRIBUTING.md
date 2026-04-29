# Contributing to ddbm

First off, thank you for considering contributing to `ddbm`! It's people like you that make the open-source community such a great place to learn, inspire, and create.

## How Can I Contribute?

### Reporting Bugs
*   Check the [Issues](https://github.com/hazeezet/dynamodb-migrator/issues) to see if the bug has already been reported.
*   If you can't find an open issue that addresses the problem, [open a new one](https://github.com/hazeezet/dynamodb-migrator/issues/new).
*   Include a clear title and as much relevant information as possible, including steps to reproduce the issue and your environment details.

### Suggesting Enhancements
*   Open a new issue with the "enhancement" tag.
*   Explain the use case and why this feature would be useful for the project.

### Pull Requests
1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/amazing-feature`).
3.  Make your changes.
4.  Ensure your code follows the project's style (run linters and formatters).
5.  Commit your changes (`git commit -m 'Add amazing feature'`).
6.  Push to the branch (`git push origin feature/amazing-feature`).
7.  Open a Pull Request.

---

## Development Setup

### Rust CLI
The high-performance CLI is located in the `rust_cli` directory.
```bash
cd rust_cli
cargo build
cargo test
```

### Python App
The flexible Python implementation is located in the `python_app` directory.
```bash
cd python_app
pip install -r requirements.txt
python run.py --help
```

## Coding Standards
*   **Rust**: Run `cargo fmt` and `cargo clippy` before submitting.
*   **Python**: Follow PEP 8 guidelines.

---

## License
By contributing, you agree that your contributions will be licensed under its [Apache License 2.0](LICENSE).
