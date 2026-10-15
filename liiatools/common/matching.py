import re
import unicodedata

_NON_WORD = re.compile(r"[\W_]+")


def normalise_text(value) -> str:
    """
    Reduces a header or category value to a canonical form for relaxed comparison.

    Applies Unicode NFKC normalisation and case folding, then replaces every run of whitespace or
    punctuation (including underscores and dashes) with a single space.
    """
    value = unicodedata.normalize("NFKC", str(value)).casefold()
    return _NON_WORD.sub(" ", value).strip()
