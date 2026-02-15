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
        return f"{result}"


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

        except Exception:
            return "[ALERT] ERROR level detected: invalid numeric data"

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

            characters = 0
            words = 0
            in_word = False

            for ch in data:
                characters += 1
                if ch != ' ' and not in_word:
                    words += 1
                    in_word = True
                elif ch == ' ':
                    in_word = False

            return f"Processed text: {characters} characters, {words} words"

        except Exception:
            return "[ALERT] ERROR level detected: invalid text data"

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

        except Exception:
            return "[ALERT] ERROR level detected: invalid log data"

    def validate(self, data: Any) -> bool:
        return type(data) is str


def demo_numeric() -> None:
    numeric_data: List[Union[int, float]] = [1, 2, 3, 4, 5]
    processor = NumericProcessor()
    print("Initializing Numeric Processor...")
    print(f"Processing data: {numeric_data}")
    if processor.validate(numeric_data):
        print("Validation: Numeric data verified")
    else:
        print("Validation Failed")
    print(f"Output: {processor.format_output(processor.process(numeric_data))}\n")


def demo_text() -> None:
    text_data = "Hello Nexus World"
    processor = TextProcessor()
    print("Initializing Text Processor...")
    print(f'Processing data: "{text_data}"')
    if processor.validate(text_data):
        print("Validation: Text data verified")
    else:
        print("Validation Failed")
    print(f"Output: {processor.format_output(processor.process(text_data))}\n")


def demo_log() -> None:
    log_data = "ERROR: Connection timeout"
    processor = LogProcessor()
    print("Initializing Log Processor...")
    print(f'Processing data: "{log_data}"')
    if processor.validate(log_data):
        print("Validation: Log entry verified")
    else:
        print("Validation Failed")
    print(f"Output: {processor.format_output(processor.process(log_data))}\n")


def demo_polymorphism() -> None:
    print("=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")

    processors: List[DataProcessor] = [NumericProcessor(), TextProcessor(), LogProcessor()]
    numeric_data: Optional[List[int]] = [1, 2, 3]
    text_data: Optional[str] = "Hello World!"
    log_data: Optional[str] = "INFO: System ready"
    data_mapping: Dict[str, Optional[Any]] = {
        "numeric": numeric_data,
        "text": text_data,
        "log": log_data
    }
    for processor, key in zip(processors, data_mapping):
        data: Optional[Any] = data_mapping[key]
        result: str = processor.process(data)
        print(f"Result ({key}): {processor.format_output(result)}")

    print("\nFoundation systems online. Nexus ready for advanced streams.")


def main() -> None:
    try:
        print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
        demo_numeric()
        demo_text()
        demo_log()
        demo_polymorphism()
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
