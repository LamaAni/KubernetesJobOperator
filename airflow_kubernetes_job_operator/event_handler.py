from typing import Dict


class EventHandler:
    message_handlers: Dict[str, Dict[int, callable]] = None
    _pipeto: [] = None
    _handler_last_idx = 0

    def __init__(self):
        """An event handler instance, that can
        collect/pipe/emit events by name, with arguments.
        """
        super().__init__()
        self._pipeto = []
        self.message_handlers = dict()
        self._handler_last_idx = 0

    def on(self, name: str, handler: callable) -> int:
        """Add a new event call.
        
        Arguments:
            name {str} -- The event name
            handler {callable} -- The method/other to
            be called when the event is triggered.
        
        Returns:
            int -- The event index, within the specific name 
            dictionary. name+index are unique.
        """
        if not self.hasEvent(name):
            self.message_handlers[name] = dict()
        idx = self._handler_last_idx
        self._handler_last_idx += 1

        self.message_handlers[name][idx] = handler
        return idx

    def hasEvent(self, name: str, index: int = None):
        """Returns true if the event is present.
        
        Arguments:
            name {str} -- The name of the event
        
        Keyword Arguments:
            index {int} -- If not None, will search for a specific
            call of this event, according to the event index returned
            from the on method. (default: {None})
        
        Returns:
            bool -- True if event exists.
        """
        if index is not None:
            return name in self.message_handlers and index in self.message_handlers[name]
        return name in self.message_handlers

    def clear(self, name: str, idx: int = None):
        """Clears an event.
        
        Arguments:
            name {str} -- The name of the event to clear.
        
        Keyword Arguments:
            idx {int} -- If not None, will clear the specific
            call to the event identified by the event index
            returned from the on method. (default: {None})
        """
        if self.hasEvent(name):
            if idx is not None:
                del self.message_handlers[name][idx]
            else:
                del self.message_handlers[name]

    def emit(self, name: str, *args, **kwargs):
        """Emits the event to all the event handler
        callable(s). Any arguments sent after name, will
        be passed to the event handler.
        
        Arguments:
            name {str} -- The name of the event to emit.
        """
        if self.hasEvent(name):
            cur_handlers = list(self.message_handlers[name].values())
            for handler in cur_handlers:
                handler(*args, **kwargs)
        for evnet_handler in self._pipeto:
            evnet_handler.emit(name, *args, **kwargs)

    def pipe(self, other):
        """Pipe all events emitted from this handler
        to another handler. This action cannot be reverted.
        
        Arguments:
            other {EventHandler} -- The event handler to pipe to.
        """
        assert isinstance(other, EventHandler), "other must be an instance of EventHandler."
        self._pipeto.append(other)
