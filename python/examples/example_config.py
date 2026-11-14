import os


def required_env(name):
    value = os.environ.get(name)
    if value:
        return value
    raise SystemExit(f"{name} must be set; see python/examples/README.md")
