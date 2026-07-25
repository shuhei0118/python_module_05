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

    def incrementRank(self) -> None:
        self._processing_rank += 1

    def getDataList(self) -> list[tuple[int, str]]:
        return self._data_list

    def output(self) -> tuple[int, str]:
        if len(self._data_list) == 0:
            return ()
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
        try:
            if isinstance(data, (int, float)):
                self._data_list.append((self._processing_rank, (str(data))))
                self._processing_rank += 1
            elif isinstance(data, (list)):
                for x in data:
                    self._data_list.append((self._processing_rank, (str(x))))
                    self._processing_rank += 1
            else:
                raise ValueError()
        except Exception:
            print("Got exception: Improper numeric data")
        return
    

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
        try:
            if isinstance(data, (str)):
                self._data_list.append((self._processing_rank, data))
                self._processing_rank += 1
            elif isinstance(data, (list)):
                for x in data:
                    self._data_list.append((self._processing_rank, (str(x))))
                    self._processing_rank += 1
            else:
                raise ValueError()
        except Exception:
            print("Got exception: Improper text data")
        return


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (dict)):
            return self.validate_dict(data)
        elif isinstance(data, (list)):
            for x in data:
                if not isinstance(x, (dict)):
                    return False
                if self.validate_dict(x) == False:
                    return False
            return True
        else:
            return False
    
    def validate_dict(self, data: dict) -> bool:
        key_list = data.keys()
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
            data: str | dict[str:str]
    ) -> None:
        try:
            if isinstance(data, (dict)):
                tmp = data['log_level']
                tmp += ': '
                tmp += data['log_message']
                self._data_list.append((self._processing_rank, tmp))
                self._processing_rank += 1
            elif isinstance(data, (list)):
                for x in data:
                    if isinstance(x, (dict)):
                        tmp = x['log_level']
                        tmp += ': '
                        tmp += x['log_message']
                        self._data_list.append((self._processing_rank, tmp) )
                        self._processing_rank += 1
            else:
                raise ValueError()
        except Exception as e:
            print(f"Got exception: Improper log data")
        return


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
    numeric_processor.ingest('foo')
    print("")
    processing_data = [1, 2, 3, 4, 5]
    if numeric_processor.validate(processing_data):
        numeric_processor.ingest(processing_data)
    data_list = [element[1] for element in numeric_processor.getDataList()]
    print(f"Processing data: {data_list}", end="\n\n")
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
    processing_data = ['Hello', 'Nexus', 'World']
    if text_processor.validate(processing_data):
        text_processor.ingest(processing_data)
    data_list = [element[1] for element in text_processor.getDataList()]
    print(f"Processing data: {data_list}", end="\n\n")
    print("Extracting 1 values...", end="\n\n")
    element = text_processor.output()
    print(f"Text value {element[0]}: {element[1]}", end="\n")

    # Testing Log Processor
    print("\nTesting Log Processor...", end="\n\n")
    log_processor = LogProcessor()
    print("Trying to validate input 'Hello': ", end="")
    print(log_processor.validate('Hello'))
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
