"""Extract the frozen pybind declarations without importing a native module."""
import re


def surface(text):
    rows = []
    for name in re.findall(r'm.attr\("(\w+)"\)', text):
        rows.append(("module", name, "constant", 1))
    for name in ["TentException", *re.findall(r'py::register_exception<\w+>\(\s*m, "(\w+)"', text)]:
        rows.append(("module", name, "exception", 1))
    for category, pattern in (
        ("class", r'py::class_<[^\n]+>\(m, "(\w+)"\)'),
        ("enum", r'py::enum_<[^\n]+>\(m, "(\w+)"\)'),
    ):
        matches = list(re.finditer(pattern, text))
        for match in matches:
            owner = match[1]
            tail = text[match.end():]
            # Bindings are complete statements; semicolons in lambdas do not
            # end the chain. The next py::class/enum declaration bounds it.
            end = re.search(r'py::(?:class_|enum_)<', tail)
            if end:
                tail = tail[:end.start()]
            if category == "enum":
                for member in re.findall(r'\.value\("(\w+)"', tail):
                    rows.append((owner, member, "enum", 1))
                continue
            constructors = len(re.findall(r'\.def\(py::init', tail))
            if constructors:
                rows.append((owner, "__init__", "constructor", constructors))
            members = {}
            for kind, member in re.findall(r'\.(def_readwrite|def_property|def)\(\s*"(\w+)"', tail):
                category = "method" if kind == "def" else "property"
                key = member, category
                members[key] = members.get(key, 0) + 1
            for (member, kind), count in members.items():
                rows.append((owner, member, kind, count))
    return sorted(rows)
