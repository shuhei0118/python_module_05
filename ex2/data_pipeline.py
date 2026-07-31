#!/usr/bin/env python3

import abc
import typing


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        """Export processed data."""
        ...


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


class CSVExportPlugin:
    @staticmethod
    def _escape(value: str) -> str:
        if any(character in value for character in (",", '"', "\n", "\r")):
            return '"' + value.replace('"', '""') + '"'
        return value

    def process_output(self, data: list[tuple[int, str]]) -> None:
        output = ",".join(self._escape(value) for _, value in data)
        print("CSV Output:")
        print(output)


class JSONExportPlugin:
    @staticmethod
    def _escape(value: str) -> str:
        result = ""
        escapes = {
            '"': '\\"',
            "\\": "\\\\",
            "\b": "\\b",
            "\f": "\\f",
            "\n": "\\n",
            "\r": "\\r",
            "\t": "\\t",
        }
        for character in value:
            if character in escapes:
                result += escapes[character]
            elif ord(character) < 0x20:
                result += "\\u" + format(ord(character), "04x")
            else:
                result += character
        return result

    def process_output(self, data: list[tuple[int, str]]) -> None:
        entries = [
            f'"item_{rank}": "{self._escape(value)}"'
            for rank, value in data
        ]
        print("JSON Output:")
        print("{" + ", ".join(entries) + "}")


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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self._processors:
            output = [
                processor.output()
                for _ in range(min(nb, len(processor._data_list)))
            ]
            plugin.process_output(output)

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
    print("\n=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("\nRegistering Processors\n")
    data_stream.register_processor(NumericProcessor())
    data_stream.register_processor(TextProcessor())
    data_stream.register_processor(LogProcessor())

    first_stream: list[typing.Any] = [
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
    print(first_stream)
    print()
    data_stream.process_stream(first_stream)
    data_stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:\n")
    data_stream.output_pipeline(3, CSVExportPlugin())
    print()
    data_stream.print_processors_stats()

    second_stream: list[typing.Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print("\nSend another batch of data:")
    print(second_stream)
    print()
    data_stream.process_stream(second_stream)
    data_stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:\n")
    data_stream.output_pipeline(5, JSONExportPlugin())
    print()
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
