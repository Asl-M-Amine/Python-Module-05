from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                return "[ALERT] ERROR level detected: invalid numeric data"
            if type(data) in (int, float):
                total = data
                length = 1
            elif type(data) is list:
                if not data:
                    return "data empty"
                total = 0
                length = 0
                for value in data:
                    total += value
                    length += 1
            else:
                return "[ALERT] ERROR level detected: invalid numeric data"
            avg = total / length
            return f"Processed {length} numeric values, sum={total}, avg={avg}"
        except Exception as e:
            return f"[ALERT] ERROR level detected: {e}"

    def validate(self, data: Any) -> bool:
        if type(data) in (int, float):
            return True
        if type(data) is list:
            for value in data:
                if type(value) not in (int, float):
                    return False
            return True
        return False


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                return "[ALERT] ERROR level detected: invalid text data"
            if not data:
                return "data empty"
            words_list = data.split()
            words_count = len(words_list)
            char_count = len(data)
            return (f"Processed text: {char_count} characters, "
                    f"{words_count} words")
        except Exception as e:
            return f"[ALERT] ERROR level detected: {e}"

    def validate(self, data: Any) -> bool:
        return type(data) is str


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                return "[ALERT] ERROR level detected: invalid log data"
            if not data:
                return "data empty"

            if data[:6] == "ERROR:":
                message = data[6:].strip()
                return "[ALERT] ERROR level detected: " + message
            elif data[:8] == "WARNING:":
                message = data[8:].strip()
                return "[WARNING] WARNING level detected: " + message
            elif data[:5] == "INFO:":
                message = data[5:].strip()
                return "[INFO] INFO level detected: " + message
            else:
                return "[INFO] INFO level detected: " + data.strip()

        except Exception as e:
            return f"[ALERT] ERROR level detected: {e}"

    def validate(self, data: Any) -> bool:
        return type(data) is str


def numeric_init() -> None:
    num_data: Optional[List[Union[int, float]]] = [1, 2, 3, 4, 5]
    processor = NumericProcessor()
    print("Initializing Numeric Processor...")
    print(f"Processing data: {num_data}")
    if processor.validate(num_data):
        print("Validation: Numeric data verified")
    else:
        print("Validation Failed")
    print(f"{processor.format_output(processor.process(num_data))}\n")


def text_init() -> None:
    text_data = "Hello Nexus World"
    processor = TextProcessor()
    print("Initializing Text Processor...")
    print(f'Processing data: "{text_data}"')
    if processor.validate(text_data):
        print("Validation: Text data verified")
    else:
        print("Validation Failed")
    print(f"{processor.format_output(processor.process(text_data))}\n")


def log_init() -> None:
    log_data = "ERROR: Connection timeout"
    processor = LogProcessor()
    print("Initializing Log Processor...")
    print(f'Processing data: "{log_data}"')
    if processor.validate(log_data):
        print("Validation: Log entry verified")
    else:
        print("Validation Failed")
    print(f"{processor.format_output(processor.process(log_data))}\n")


def Polymorphic_handle() -> None:
    print("=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...\n")

    processors = [NumericProcessor(), TextProcessor(), LogProcessor()]
    data_mapping: Dict[str, Union[List[int], str]] = {
        "numeric": [1, 2, 3],
        "text": "Hello World!",
        "log": "INFO: System ready"
    }

    keys = list(data_mapping.keys())
    i = 1
    for processor in processors:
        key = keys[i - 1]
        data = data_mapping[key]
        result = processor.process(data)
        print(f"Result {i}: {result}")
        i += 1


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    numeric_init()
    text_init()
    log_init()
    Polymorphic_handle()
    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Unexpected error: {e}")
