# Contributing to DynamoDBv2.Transactions

Thank you for considering contributing to **DynamoDBv2.Transactions**! We appreciate your time and effort in helping improve this project. This document outlines the process for contributing to ensure a smooth collaboration.

## Getting Started

### 1. Fork & Clone the Repository
1. Fork the repository on GitHub.
2. Clone your fork:
   ```sh
   git clone https://github.com/your-username/DynamoDBv2.Transactions.git
   ```
3. Navigate into the project directory:
   ```sh
   cd DynamoDBv2.Transactions
   ```
4. Add the upstream repository:
   ```sh
   git remote add upstream https://github.com/vitalybibikov/DynamoDBv2.Transactions.git
   ```

### 2. Set Up the Development Environment
1. Ensure you have the following installed:
   - [.NET SDK](https://dotnet.microsoft.com/download)
   - [Git](https://git-scm.com/)
2. Restore dependencies:
   ```sh
   dotnet restore
   ```
3. Build the project:
   ```sh
   dotnet build
   ```
4. Run tests:
   ```sh
   dotnet test
   ```

## Making Changes

### 1. Create a New Branch
Before making any changes, create a new feature branch:
```sh
   git checkout -b feature/your-feature-name
```

### 2. Implement Your Changes
- Follow the existing coding style.
- Write unit tests for new features or bug fixes.
- Ensure all tests pass before submitting a PR.

### 3. Commit Your Changes
Follow conventional commit messages:
```sh
git commit -m "feat: Add new feature description"
git commit -m "fix: Resolve issue #123"
```

### 4. Push and Open a Pull Request
1. Push your branch:
   ```sh
   git push origin feature/your-feature-name
   ```
2. Go to your fork on GitHub and click **New Pull Request**.
3. Select the upstream repository (`vitalybibikov/DynamoDBv2.Transactions`) and the `main` branch as the base.
4. Provide a clear title and description for your PR.

## Code of Conduct
By contributing, you agree to adhere to the [Code of Conduct](CODE_OF_CONDUCT.md) to ensure a welcoming and inclusive community.

## Reporting Issues
If you find a bug or have a feature request, please [open an issue](https://github.com/vitalybibikov/DynamoDBv2.Transactions/issues) and provide details.

## License
By contributing, you agree that your contributions will be licensed under the project's existing license.

Thank you for your contributions! 🚀

