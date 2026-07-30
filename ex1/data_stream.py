#!/usr/bin/env python3

import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._processing_rank = 0
        self._data_list: list[tuple[int, str]] = []

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def _store(self, value: str) -> None:
        self._data_list.append((self._processing_rank, value))
        self._processing_rank += 1

    def output(self) -> tuple[int, str]:
        return self._data_list.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)
        return False

    def ingest(
        self, data: int | float | list[int | float]
    ) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._store(str(item))
        else:
            self._store(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._store(item)
        else:
            self._store(data)


class LogProcessor(DataProcessor):
    def _validate_dict(self, data: dict[typing.Any, typing.Any]) -> bool:
        return (
            set(data.keys()) == {"log_level", "log_message"}
            and isinstance(data["log_level"], str)
            and isinstance(data["log_message"], str)
        )

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return self._validate_dict(data)
        if isinstance(data, list):
            return all(
                isinstance(item, dict) and self._validate_dict(item)
                for item in data
            )
        return False

    def ingest(
        self, data: dict[str, str] | list[dict[str, str]]
    ) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            self._store(
                f"{entry['log_level']}: {entry['log_message']}"
            )


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            for processor in self._processors:
                if processor.validate(element):
                    processor.ingest(element)
                    break
            else:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{element}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for processor in self._processors:
            name = processor.__class__.__name__.replace(
                "Processor", " Processor"
            )
            print(f"\n{name}:")
            print(f"    total {processor._processing_rank} items processed,")
            print(
                f"    remaining {len(processor._data_list)} on processor"
            )


def main() -> None:
    print("\n=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...\n")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    numeric_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()

    print("\nRegistering Numeric Processor\n")
    data_stream.register_processor(numeric_processor)

    stream: list[typing.Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected",
            },
        ],
        42,
        ["Hi", "five"],
    ]

    print("Send first batch of data on stream:")
    print(stream)
    print()
    data_stream.process_stream(stream)
    print()
    data_stream.print_processors_stats()

    print("\nRegistering other data processors\n")
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)

    print("Send the same batch again\n")
    data_stream.process_stream(stream)
    data_stream.print_processors_stats()

    print("\nConsume some elements from the data processors:")
    print("Numeric 3, Text 2, Log 1\n")
    for _ in range(3):
        numeric_processor.output()
    for _ in range(2):
        text_processor.output()
    log_processor.output()

    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
