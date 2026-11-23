"""Value-free, structured errors for authored configuration."""

from dataclasses import asdict, dataclass, replace


@dataclass(frozen=True)
class ManifestIssue:
    code: str
    message: str
    document: int = 1
    path: str = "/"
    #: The named source the document came from, when the input was several
    #: sources; ``document`` then counts inside that source.
    source: str | None = None


class ManifestError(ValueError):
    def __init__(self, issue: ManifestIssue):
        self.issue = issue
        super().__init__(issue.message)

    def as_dict(self) -> dict:
        issue = asdict(self.issue)
        if issue["source"] is None:
            del issue["source"]
        return issue

    def in_source(self, name: str) -> "ManifestError":
        """This error, raised while reading the source called ``name``."""
        return ManifestError(replace(self.issue, source=name))

    def located(self, origins) -> "ManifestError":
        """This error with its document traced back to a named source.

        ``origins`` holds, per document of a bundle read from several
        sources, the source's name and the document's number inside it
        (``parsing.ParsedSources.origins``). An error raised after parsing
        counts documents across the whole bundle; this turns that number into
        the pair a person can find in their files. An error that already
        names a source, or whose number is outside the bundle, is returned
        unchanged.
        """
        number = self.issue.document
        if self.issue.source is not None or not 1 <= number <= len(origins):
            return self
        name, local = origins[number - 1]
        return ManifestError(replace(self.issue, source=name, document=local))


def fail(code: str, message: str, *, document: int = 1, path: str = "/"):
    raise ManifestError(ManifestIssue(code, message, document, path))


def pointer(parts) -> str:
    return "/" + "/".join(str(p).replace("~", "~0").replace("/", "~1") for p in parts)
