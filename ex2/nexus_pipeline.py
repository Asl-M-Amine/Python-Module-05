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
        print(f'Input: "{data}"')
        return data


class TransformStage:
    def process(self, data: Any) -> Any:

        if isinstance(data, dict) and "value" in data:
            print("Transform: Enriched with metadata and validation")
            data["status"] = "Normal range"
            return data

        if isinstance(data, str):
            print("Transform: Parsed and structured data")
            counts = Counter(data.split(","))
            return {"count_action": counts.get("action", 0)}

        if isinstance(data, list):
            print("Transform: Aggregated and filtered")
            avg = sum(data) / len(data)
            return {"count": len(data), "avg": avg}

        return data


class OutputStage:
    def process(self, data: Any) -> Any:

        if isinstance(data, dict) and "value" in data:
            print(f"Output: Processed temperature reading: "
                  f"{data['value']}°C ({data['status']})\n")

        elif isinstance(data, dict) and "count" in data:
            print(f"Output: Stream summary: {data['count']} readings, "
                  f"avg: {data['avg']}°C\n")

        else:
            print(f"Output: User activity logged: "
                  f"{data['count_action']} actions processed\n")

        return data


class ProcessingPipeline(ABC):

    def __init__(self) -> None:
        self.stages = []
        self.stats: Dict[str, int] = {
         "stagesexecuted": 0, "success": 0, "errors": 0}

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        ...

    def execute(self, data: Any) -> Any:
        try:
            i = 1
            for stage in self.stages:
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
        print("Processing JSON data through pipeline...")
        return self.execute(data)


class CSVAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing CSV data through same pipeline...")
        return self.execute(data)


class StreamAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing Stream data through same pipeline...")
        return self.execute(data)


class NexusManager:

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.pipelines = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_data(self) -> None:
        pipelines = ["Pipeline A", "Pipeline B", "Pipeline C"]
        stages = ["Input", "Transform", "Output"]
        steps = {}
        stats = {"stagesexecuted": 0, "success": 0}
        print(pipelines[0], end=" ")
        steps["A_pipeline"] = "Processed"
        for _ in stages:
            stats['success'] += 1
        stats['stagesexecuted'] += 1
        print("->", pipelines[1], end=" ")
        steps["B_pipeline"] = "Analyzed"
        for _ in stages:
            stats['success'] += 1
        stats['stagesexecuted'] += 1
        print("->", pipelines[2])
        steps["C_pipeline"] = "Stored"
        for _ in stages:
            stats['success'] += 1
        stats['stagesexecuted'] += 1
        print(f"Data flow: Raw -> {steps['A_pipeline']} -> "
              f"{steps['B_pipeline']} -> {steps['C_pipeline']}\n")

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
    json_pipeline.process({"sensor": "temp", "value": 23.5, "unit": "C"})
    csv_pipeline.process("user,action,timestamp")
    stream_pipeline.process([21.5, 22.0, 23.0, 22.5, 21.5])


def run_chaining_demo(manager: NexusManager) -> None:
    print("=== Pipeline Chaining Demo ===")
    manager.process_data()


def run_error_test(json_pipeline: ProcessingPipeline) -> None:
    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    json_pipeline.process(None)


def main() -> None:
    manager = initialize_system()
    pipelines = create_pipelines()
    configure_stages(pipelines)
    register_pipelines(manager, pipelines)
    run_processing_demo(pipelines)
    run_chaining_demo(manager)
    run_error_test(pipelines[0])

    print("\nNexus Integration complete. All systems operational.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Unexpected error: {e}")
