# PySpark Testing Project

## Overview
This project provides a framework for testing PySpark applications. It includes unit tests, integration tests, and best practices for ensuring PySpark jobs run correctly and efficiently.

## Features
- Unit testing with `pytest` and `unittest`.
- Data validation using `Great Expectations`.
- Mocking Spark DataFrames for testing.
- CI/CD integration for automated testing.

## Installation

### Prerequisites
- Python 3.8+
- Apache Spark
- PySpark
- pytest
- unittest
- Great Expectations

### Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Project Structure
```
/pyspark-testing-project
│── src/
│   ├── job.py  # Main PySpark Job
│   ├── utils.py  # Utility functions
│── tests/
│   ├── test_job.py  # Unit tests for job.py
│   ├── test_utils.py  # Unit tests for utils.py
│── requirements.txt  # Project dependencies
│── README.md  # Project documentation
```

## Running Tests

### Run All Tests
```bash
pytest tests/
```

### Run Specific Test
```bash
pytest tests/test_job.py
```

### Using Unittest
```bash
python -m unittest discover tests
```

## Best Practices
- Use small sample DataFrames for unit testing.
- Mock SparkSession to avoid heavy initialization.
- Validate data transformations with predefined test cases.
- Integrate testing into CI/CD pipelines.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request.

## License
This project is licensed under the MIT License.

