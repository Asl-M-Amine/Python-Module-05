from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.processed_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "processed_items": self.processed_count
        }


class SensorStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count = len(data_batch)
            alerts = sum(1 for d in data_batch if d.get("temp", 0) > 30
                         or d.get("temp", 0) < 0)
            if alerts:
                return "[ALERT] found extern-values!!"
            total_temp = sum(d.get("temp", 0.0) for d in data_batch)
            avg_temp = (total_temp / self.processed_count
                        if self.processed_count else 0.0)
            return (f"Sensor analysis: {self.processed_count} readings "
                    f"processed, avg temp: {avg_temp}°C")
        except Exception as e:
            return f"Sensor processing error: {e}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "high-alert":
            return [d for d in data_batch if d.get("temp", 0) > 30]
        elif criteria == "large":
            return [d for d in data_batch if d.get("temp", 0) > 15]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "Environmental Data"
        stats["process"] = "Sensor data"
        return stats


class TransactionStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count = len(data_batch)
            sum_buy = sum(d.get("buy", 0) for d in data_batch)
            sum_sell = sum(d.get("sell", 0) for d in data_batch)
            net_flow = sum_buy - sum_sell
            sign = "+" if net_flow >= 0 else ""
            return (f"Transaction analysis: {self.processed_count} operations,"
                    f" net flow: {sign}{net_flow} units")
        except Exception as e:
            return f"Transaction processing error: {e}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "high":
            return [d for d in data_batch if d.get("buy", 0) > 200
                    or d.get("sell", 0) > 200]
        elif criteria == "large":
            return [d for d in data_batch if d.get("buy", 0) > 100
                    or d.get("sell", 0) > 100]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "Financial Data"
        stats["process"] = "Transaction data"
        return stats


class EventStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count = len(data_batch)
            error_count = sum(1 for event in data_batch if "error" in event)
            return (f"Event analysis: {self.processed_count} "
                    f"events, {error_count} error detected")
        except Exception as e:
            return f"Event processing error: {e}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "errors":
            return [event for event in data_batch if "error" in event]
        elif criteria == "login":
            return [event for event in data_batch if "login" in event]
        elif criteria == "logout":
            return [event for event in data_batch if "logout" in event]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["type"] = "System Events"
        stats["process"] = "Event data"
        return stats


class StreamProcessor:
    def __init__(self) -> None:
        self.streams = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_all(self, data_batches: Dict[str, Any]) -> None:
        try:
            print("\n=== Polymorphic Stream Processing ===")
            print("Processing mixed stream types through unified interface...")
            print("\nBatch 1 Results:")
            for stream in self.streams:
                if isinstance(stream, SensorStream):
                    mod = "readings"
                elif isinstance(stream, TransactionStream):
                    mod = "operations"
                elif isinstance(stream, EventStream):
                    mod = "events"
                else:
                    mod = ""
                batch = data_batches.get(stream.stream_id, [])
                stream.process_batch(batch)
                stats = stream.get_stats()
                print(f"- {stats['process']}: {stats['processed_items']} "
                      f"{mod} processed")
        except Exception as e:
            print(f"Stream failure: {e}")


def initialize_sensor() -> SensorStream:
    print("\nInitializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    stats = sensor.get_stats()
    print(f"Stream ID: {stats['stream_id']}, Type: {stats['type']}")
    batch = [{"temp": 22.5, "humidity": 65, "pressure": 1013}]
    print("Processing sensor batch: "
          f"[temp:{batch[0]['temp']}, "
          f"humidity:{batch[0]['humidity']}, "
          f"pressure:{batch[0]['pressure']}]")
    print(sensor.process_batch(batch))
    return sensor


def initialize_transaction() -> TransactionStream:
    print("\nInitializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    stats = transaction.get_stats()
    print(f"Stream ID: {stats['stream_id']}, Type: {stats['type']}")
    batch = [{"buy": 100, "sell": 0},
             {"buy": 0, "sell": 150},
             {"buy": 75, "sell": 0}]
    print("Processing transaction batch: "
          f"[buy:{batch[0]['buy']}, sell:{batch[1]['sell']}"
          f", buy:{batch[2]['buy']}]")
    print(transaction.process_batch(batch))
    return transaction


def initialize_event() -> EventStream:
    print("\nInitializing Event Stream...")
    event = EventStream("EVENT_001")
    stats = event.get_stats()
    print(f"Stream ID: {stats['stream_id']}, Type: {stats['type']}")
    batch_event = ["login", "error", "logout"]
    print(f"Processing event batch_event: [{batch_event[0]}, "
          f"{batch_event[1]}, {batch_event[2]}]")
    print(event.process_batch(batch_event))
    return event


def polymorphic_processing(sensor: SensorStream,
                           transaction: TransactionStream,
                           event: EventStream) -> None:

    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(transaction)
    processor.add_stream(event)

    data_batches = {
        "SENSOR_001": [{"temp": 40.5}, {"temp": 32.0}],
        "TRANS_001": [{"buy": 120, "sell": 0},
                      {"buy": 25, "sell": 0},
                      {"buy": 0, "sell": 75},
                      {"buy": 10, "sell": 0}],
        "EVENT_001": ["login", "error", "logout"]
    }
    processor.process_all(data_batches)

    print("\nStream filtering active: High-priority data only")
    filter_sen = sensor.filter_data(data_batches["SENSOR_001"], "high-alert")
    filter_tran = transaction.filter_data(data_batches["TRANS_001"], "large")
    print(f"Filtered results: {len(filter_sen)} critical sensor alerts, "
          f"{len(filter_tran)} large transaction")

    print("\nAll streams processed successfully. Nexus throughput optimal.")


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    sensor = initialize_sensor()
    transaction = initialize_transaction()
    event = initialize_event()

    polymorphic_processing(sensor, transaction, event)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Unexpected error: {e}")
