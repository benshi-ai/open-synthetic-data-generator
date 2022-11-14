from synthetic.sink.csv_flush_sink import CSVFlushSink
from synthetic.sink.flush_sink import FlushSink
from synthetic.sink.http_flush_sink import HTTPFlushSink
from synthetic.sink.memory_flush_sink import MemoryFlushSink


def build_sink_from_type(sink_type: str) -> FlushSink:
    if sink_type == "csv":
        return CSVFlushSink()
    elif sink_type == "http":
        return HTTPFlushSink()
    elif sink_type == "memory":
        return MemoryFlushSink()
    else:
        raise ValueError("Invalid sink type: %s" % (sink_type,))
