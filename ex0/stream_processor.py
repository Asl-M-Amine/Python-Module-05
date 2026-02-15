from typing import Any
from abc import ABC, abstractmethod


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
        if not self.validate(data):
            return "[ALERT] ERROR level detected: invalid numeric data"
        if data:
            total = 0
            length = 0
            for i in data:
                total += i
                length += 1
        else:
            return "data empty"
        avg = total / length
        return f"Processed {length} numeric values, sum={total}, avg={avg}"

    def validate(self, data: Any) -> bool:
        for i in data:
            if not isinstance(i, (int, float)):
                return False
        return True

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "[ALERT] ERROR level detected: invalid text data"
        if data:
            count_word = 0
            character = 0
            in_word = True
            for i in data:
                if i == ' ' and in_word is False:
                    in_word = True
                elif i != ' ' and in_word is True:
                    count_word += 1
                    in_word = False
                character += 1
        else:
            return "data empty"
        return f"Processed text: {character} characters, {count_word} words"

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "[ALERT] ERROR level detected: invalid log data"
        if not data:
            return "data empty"
        if data[:6] == "ERROR:":
            message = data[6:]
            return "[ALERT] ERROR level detected: " + message
        if data[:8] == "WARNING:":
            message = data[8:]
            return "[WARNING] WARNING level detected: " + message
        if data[:5] == "INFO:":
            message = data[5:]
            return "[INFO] INFO level detected: " + message
        return "[INFO] INFO level detected: " + data

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def format_output(self, result: str) -> str:
        return super().format_output(result)



def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    numeric_data = [1, 2, 3, 4, 5]
    num_proc = NumericProcessor()
    result = num_proc.process(numeric_data)
    print(f"Processing data: {numeric_data}")
    if num_proc.validate(numeric_data):
        print("Validation: Numeric data verified")
    else:
        print("Validation Failed")
    print(f"Output: {num_proc.format_output(result)}")
    print()

    print("Initializing Text Processor...")
    text_data = "Hello Nexus World"
    text_proc = TextProcessor()
    result = text_proc.process(text_data)
    print(f'Processing data: "{text_data}"')
    if text_proc.validate(text_data):
        print("Validation: Text data verified")
    else:
        print("Validation Failed")
    print(f"Output: {text_proc.format_output(result)}")
    print()

    print("Initializing Log Processor...")
    log_data = "ERROR: Connection timeout"
    log_proc = LogProcessor()
    result = log_proc.process(log_data)
    print(f'Processing data: "{log_data}"')
    if log_proc.validate(log_data):
        print("Validation: Log entry verified")
    else:
        print("Validation Failed")
    print(f"Output: {log_proc.format_output(result)}")
    print()

    print("=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")
    numeric_data = [1, 2, 3]
    text_data = "Hello World!"
    log_data = "INFO: System ready"
    print(f"Result 1: {num_proc.format_output(num_proc.process(numeric_data))}")
    print(f"Result 2: {text_proc.format_output(text_proc.process(text_data))}")
    print(f"Result 3: {log_proc.format_output(log_proc.process(log_data))}")
    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Unexpected error: {e}")