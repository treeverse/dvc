from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    import builtins


class dict:  # noqa: A001, N801, PLW1641
    """Special class to eq by matching only presented dict keys.

    Implementation notes:

     - can't inherit from dict because that makes D() == M.dict() to not call
       our __eq__, if D is a subclass of a dict

     - should not call itself dict or use dict in repr because it creates
       confusing error messages (shadowing python builtins is bad anyway)

    """

    def __init__(self, d: Optional[Mapping[Any, Any]] = None, **keys: Any) -> None:
        self.d: builtins.dict[Any, Any] = {}
        if d:
            self.d.update(d)
        self.d.update(keys)

    def __len__(self) -> int:
        return len(self.d)

    def __repr__(self) -> str:
        inner = ", ".join(f"{k}={v!r}" for k, v in self.d.items())
        return f"{self.__class__.__name__}({inner})"

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, Mapping)
        return all(other.get(name) == v for name, v in self.d.items())


class unordered:  # noqa: N801, PLW1641
    """Compare list contents, but do not care about ordering.

    (E.g. sort lists first, then compare.)
    If you care about ordering, then just compare lists directly."""

    def __init__(self, *items: Any) -> None:
        self.items = items

    def __repr__(self) -> str:
        inner = ", ".join(map(repr, self.items))
        return f"{self.__class__.__name__}({inner})"

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, Iterable)
        return sorted(self.items) == sorted(other)


class attrs:  # noqa: N801, PLW1641
    def __init__(self, **attribs: Any) -> None:
        self.attribs = attribs

    def __repr__(self) -> str:
        inner = ", ".join(f"{k}={v!r}" for k, v in self.attribs.items())
        return f"{self.__class__.__name__}({inner})"

    def __eq__(self, other: object) -> bool:
        # Unforturnately this doesn't work with classes with slots
        # self.__class__ = other.__class__
        return all(getattr(other, name) == v for name, v in self.attribs.items())


__all__ = [
    "attrs",
    "dict",
    "unordered",
]
