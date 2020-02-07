from typing import Dict


class EventHandler:
    message_handlers: Dict[str, Dict[int, object]] = None
    _pipeto: [] = None
    _handler_last_idx = 0

    def __init__(self):
        super().__init__()
        self._pipeto = []
        self.message_handlers = dict()
        self._handler_last_idx = 0

    def on(self, name, handler):
        if not self.hasEvent(name):
            self.message_handlers[name] = dict()
        idx = self._handler_last_idx
        self._handler_last_idx += 1

        self.message_handlers[name][idx] = handler
        return idx

    def hasEvent(self, name):
        return name in self.message_handlers

    def clear(self, name, idx: int = None):
        if self.hasEvent(name):
            if idx is not None:
                del self.message_handlers[name][idx]
            else:
                del self.message_handlers[name]

    def emit(self, name, *args, **kwargs):
        if self.hasEvent(name):
            cur_handlers = list(self.message_handlers[name].values())
            for handler in cur_handlers:
                handler(*args, **kwargs)
        for evnet_handler in self._pipeto:
            evnet_handler.emit(name, *args, **kwargs)

    def pipe(self, other):
        assert isinstance(other, EventHandler)
        self._pipeto.append(other)
