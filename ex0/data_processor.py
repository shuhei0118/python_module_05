#!/usr/bin/env python3

from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._processing_rank = 0
        self._data_list: list[tuple[int, str]] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def _store(self, value: str) -> None:
        self._data_list.append((self._processing_rank, str(value)))
        self._processing_rank += 1

    def output(self) -> tuple[int, str]:
        return self._data_list.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, (list)):
            for x in data:
                if not isinstance(x, (int, float)):
                    return False
            return True
        else:
            return False

    def ingest(
            self,
            data: int | float | list[int | float]
    ) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, (int, float)):
            self._store(str(data))
        else:
            for item in data:
                self._store(str(item))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (str)):
            return True
        elif isinstance(data, (list)):
            for x in data:
                if not isinstance(x, str):
                    return False
            return True
        else:
            return False

    def ingest(
            self,
            data: str | list[str]
    ) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, str):
            self._store(data)
        else:
            for x in data:
                self._store(x)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (dict)):
            return self.validate_dict(data)
        elif isinstance(data, (list)):
            for x in data:
                if not isinstance(x, (dict)):
                    return False
                if self.validate_dict(x) is False:
                    return False
            return True
        else:
            return False

    def validate_dict(self, data: dict[str, str]) -> bool:
        value_list = data.values()
        if 'log_level' not in data:
            return False
        if 'log_message' not in data:
            return False
        for x in value_list:
            if not isinstance(x, str):
                return False
        return True

    def ingest(
            self,
            data: dict[str, str] | list[dict[str, str]]
    ) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, dict):
            text = f"{data['log_level']}: {data['log_message']}"
            self._store(text)
        else:
            for item in data:
                text = f"{item['log_level']}: {item['log_message']}"
                self._store(text)


def main() -> None:
    print("\n=== Code Nexus - Data Processor ===", end="\n\n")
    # Testing Numeric Processor
    print("Testing Numeric Processor...", end="\n\n")
    numeric_processor = NumericProcessor()
    print("Trying to validate input '42': ", end="")
    print(numeric_processor.validate(42))
    print("Trying to validate input '0.5': ", end="")
    print(numeric_processor.validate(0.5))
    print("Trying to validate input '[1,2]': ", end="")
    print(numeric_processor.validate([1, 2]))
    print("Trying to validate input '[1,0.5]': ", end="")
    print(numeric_processor.validate([1, 0.5]))
    print("Trying to validate input '[1,0.5,'nomura']': ", end="")
    print(numeric_processor.validate([1, 2, 'nomura']))
    print("Trying to validate input 'Hello': ", end="")
    print(numeric_processor.validate('Hello'))
    print("")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric_processor.ingest("foo")  # type: ignore[arg-type]
    except ValueError as e:
        print(f"Got exception: {e}")
    print("")
    processing_data = [1, 2, 3, 4, 5]
    print(f"Processing data: {processing_data}", end="\n\n")
    if numeric_processor.validate(processing_data):
        numeric_processor.ingest(processing_data)
    print("Extracting 3 values...", end="\n\n")
    for i in range(0, 3):
        element = numeric_processor.output()
        print(f"Numeric value {element[0]}: {element[1]}")

    # Testing Text Processor
    print("\nTesting Text Processor...", end="\n\n")
    text_processor = TextProcessor()
    print("Trying to validate input '42': ", end="")
    print(text_processor.validate(42))
    print("")
    print("Test invalid ingestion of integer 42 without prior validation:")
    try:
        text_processor.ingest(42)  # type: ignore[arg-type]
    except ValueError as e:
        print(f"Got exception: {e}")
    print("")
    processing_data = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {processing_data}", end="\n\n")
    if text_processor.validate(processing_data):
        text_processor.ingest(processing_data)
    print("Extracting 1 values...", end="\n\n")
    element = text_processor.output()
    print(f"Text value {element[0]}: {element[1]}", end="\n")

    # Testing Log Processor
    print("\nTesting Log Processor...", end="\n\n")
    log_processor = LogProcessor()
    print("Trying to validate input 'Hello': ", end="")
    print(log_processor.validate('Hello'))
    print("")
    print("Test invalid ingestion of string 'Hello' without prior validation:")
    try:
        log_processor.ingest("Hello")  # type: ignore[arg-type]
    except ValueError as e:
        print(f"Got exception: {e}")
    print("")
    processing_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f"Processing data: {processing_data}", end="\n\n")
    if log_processor.validate(processing_data):
        log_processor.ingest(processing_data)
    print("Extracting 2 values...", end="\n\n")
    element = log_processor.output()
    print(f"Log entry {element[0]}: {element[1]}", end="\n")
    element = log_processor.output()
    print(f"Log entry {element[0]}: {element[1]}", end="\n")


if __name__ == "__main__":
    main()
