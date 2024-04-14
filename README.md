# VK Data Fetch

This repository contains a personal project developed for educational purposes, focusing on data scraping and analysis from VK, a well-known social networking service. The project is not currently under active development but is available publicly on GitHub to demonstrate my experience with such projects.

## Project Overview

- **Programming Language:** Python
- **Package Management:** pip

## Configuration and Usage

### Initial Setup

Define your configuration settings in `todo.yml`. Further details on configuration options are provided within the file.

Modification and expansion of available methods are possible by editing `methods.yml`.

### Data Fetching

To initiate data fetching, execute the fetcher module. Optional command-line arguments include:
- `--skip-fetcher` to bypass data fetching
- `--skip-merger` to bypass data merging
- `--skip-ml` to omit the machine learning training step
- `--model-path` to designate a specific path for the FastText model

## Machine Learning Features

The project incorporates machine learning to process and analyze text data from VK through the use of embeddings, with the aim of identifying similarities between groups and users.

- **Text Processing**: Text data undergoes filtering and preprocessing based on language.
- **Embedding Generation**: Embeddings are generated if a FastText model path is specified (`--model-path`), by averaging sentence vectors from the processed text.
- **Activation**: Use the `--model-path` option to enable machine learning features. The `--skip-ml` option allows for skipping this phase.

## Disclaimer

This project is independently created and is neither officially affiliated with nor endorsed by VK or any of its affiliates. It is provided as-is, and users assume all risks associated with its use.

## License

Licensed under the MIT License.
