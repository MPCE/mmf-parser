from datetime import date

class DupeDict(dict):
    """A dict that prevents overwriting existing values.

    Keys are automatically converted to strings, and renamed if
    they already exist in the dict.

    Example:
        >>> foo = DupeDict()
        >>> foo['bar'] = 1
        >>> foo['bar'] = 2
        >>> foo
        {'bar':1,'bar1':2}"""


    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        key = str(key)
        i = 0
        if key in self:
            i += 1
            self.__setitem__(key + str(i), value)
        else:
            super(DupeDict, self).__setitem__(key, value)

    def update(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError(
                    f"update expected at most 1 arguments, got {len(args)}")
            for key, value in args[0]:
                self.__setitem__(key, value)
        for key in kwargs:
            self[key] = kwargs[key]
        
class ListDict(dict):
    """A dict that prevents overwriting existing values.

    If the value of an existing key is updated, the old value is
    converted to a list and the new value is appended to it.

    Example:
        >>> foo = ListDict()
        >>> foo['bar'] = 1
        >>> foo['bar'] = 2
        >>> foo['baz'] = 77
        >>> foo
        {'bar':[1,2], 'baz':77}"""
    
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
    
    def __setitem__(self, key, value):
        if key in self:
            super(ListDict, self).__setitem__(key, [self[key]])
            self[key].append(value)
        else:
            super(ListDict, self).__setitem__(key, value)

    def update(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError(f"update expected at most 1 arguments, got {len(args)}")
            for key, value in args[0]:
                self.__setitem__(key, value)
        for key in kwargs:
            self[key] = kwargs[key]


class ErrorDict(dict):
    """A dict specifically for inserting errors into the MMF2 database."""

    def __init__(self, inputtext):
        self.update(
            filename = inputtext,
            date = date.today()
        )
        self.reset()
    
    def reset(self):
        self.update(
            edition_id=None,
            work_id=None,
            text=None,
            error_note=None
        )
