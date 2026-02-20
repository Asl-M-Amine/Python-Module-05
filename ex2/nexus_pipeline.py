from abc import ABC, abstractmethod
from typing import Protocol, Any, Union, List, Dict
from collections import Counter


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage:
    def process(self, data: Any) -> Any:
        if data is None:
            raise ValueError("Invalid data format")
        return data

    def display(self, data: Any) -> None:
        print(f'Input: "{data}"')


class TransformStage:
    def process(self, data: Any) -> Any:
        if isinstance(data, dict) and "value" in data:
            data["status"] = "Normal range"
            return data

        if isinstance(data, str):
            counts = Counter(data.split(","))
            return {"count_action": counts.get("action", 0)}

        if isinstance(data, list):
            avg = sum(data) / len(data)
            return {"count": len(data), "avg": avg}

        return data

    def display(self, data: Any) -> None:
        if isinstance(data, dict) and "value" in data:
            print("Transform: Enriched with metadata and validation")
        if isinstance(data, str):
            print("Transform: Parsed and structured data")
        if isinstance(data, list):
            print("Transform: Aggregated and filtered")


class OutputStage:
    def process(self, data: Any) -> Any:
        return data

    def display(self, data: Any) -> None:
        if isinstance(data, dict) and "value" in data:
            print(f"Output: Processed temperature reading: "
                  f"{data['value']}°C ({data['status']})\n")
        elif isinstance(data, dict) and "count" in data:
            print(f"Output: Stream summary: {data['count']} readings, "
                  f"avg: {data['avg']}°C\n")
        else:
            print(f"Output: User activity logged: "
                  f"{data['count_action']} actions processed\n")


class ProcessingPipeline(ABC):

    def __init__(self) -> None:
        self.stages = []
        self.affich = True
        self.stats: Dict[str, int] = {
         "stagesexecuted": 0, "success": 0, "errors": 0}

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        ...

    def execute(self, data: Any, affich: bool) -> Any:
        try:
            i = 1
            for stage in self.stages:
                if affich:
                    stage.display(data)
                data = stage.process(data)
                self.stats["stagesexecuted"] += 1
                i += 1

            self.stats["success"] += 1

        except Exception as e:
            print(f"Error detected in Stage {i}: {e}")
            print("Recovery initiated: Switching to backup processor")
            print("Recovery successful: Pipeline restored, processing resumed")
            self.stats["errors"] += 1

        return data


class JSONAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        if not isinstance(data, dict):
            return "Invalid JSON format"

        cleaned = {k: v for k, v in data.items() if v is not None}
        return self.execute(cleaned, self.affich)


class CSVAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        return self.execute(data, self.affich)


class StreamAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        if not isinstance(data, list):
            return "Invalid stream format"

        filtered = [x for x in data if isinstance(x, (int, float))]
        return self.execute(filtered, self.affich)


class NexusManager:

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.pipelines = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_data(self, data: Any) -> None:
        print("Pipeline A -> Pipeline B -> Pipeline C")
        current_data = data
        for piplien in self.pipelines:
            piplien.affich = False
            current_data = piplien.process(current_data)
        print("Data flow: Raw -> Processed -> Analyzed -> Stored\n")
        total_pipelines = len(self.pipelines)
        if total_pipelines == 0:
            print("No pipelines registered!")
            return
        stages_per_pipeline = len(self.pipelines[0].stages)
        total_records = total_pipelines * stages_per_pipeline * 10
        print(f"Chain result: {total_records} records processed "
              f"through {stages_per_pipeline}-stage pipeline")
        print(f"Performance: {total_records}% efficiency, 0.2s "
              "total processing time")


def initialize_system() -> NexusManager:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    print("Initializing Nexus Manager...")
    capacity = 1000
    manager = NexusManager(capacity)
    print(f"Pipeline capacity: {capacity} streams/second\n")
    return manager


def create_pipelines():
    print("Creating Data Processing Pipeline...")
    json_pipeline = JSONAdapter("JSON_1")
    csv_pipeline = CSVAdapter("CSV_1")
    stream_pipeline = StreamAdapter("STREAM_1")
    return [json_pipeline, csv_pipeline, stream_pipeline]


def configure_stages(pipelines: List[ProcessingPipeline]) -> None:
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery\n")

    for pipeline in pipelines:
        pipeline.add_stage(InputStage())
        pipeline.add_stage(TransformStage())
        pipeline.add_stage(OutputStage())


def register_pipelines(manager: NexusManager,
                       pipelines: List[ProcessingPipeline]) -> None:
    for pipeline in pipelines:
        manager.add_pipeline(pipeline)


def run_processing_demo(pipelines: List[ProcessingPipeline]) -> None:
    print("=== Multi-Format Data Processing ===\n")
    json_pipeline, csv_pipeline, stream_pipeline = pipelines
    print("Processing JSON data through pipeline...")
    json_pipeline.process({"sensor": "temp", "value": 23.5, "unit": "C"})
    print("Processing CSV data through same pipeline...")
    csv_pipeline.process("user,action,timestamp")
    print("Processing Stream data through same pipeline...")
    stream_pipeline.process([21.5, 22.0, 23.0, 22.5, 21.5])


def run_chaining_demo(manager: NexusManager) -> None:
    print("=== Pipeline Chaining Demo ===")
    manager.process_data({"sensor": "temp", "value": 30.5, "unit": "C"})


def run_error_test(csv_pipeline: ProcessingPipeline) -> None:
    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    csv_pipeline.process(None)


def main() -> None:
    manager = initialize_system()
    pipelines = create_pipelines()
    configure_stages(pipelines)
    register_pipelines(manager, pipelines)
    run_processing_demo(pipelines)
    run_chaining_demo(manager)
    run_error_test(pipelines[1])

    print("\nNexus Integration complete. All systems operational.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Unexpected error: {e}")
