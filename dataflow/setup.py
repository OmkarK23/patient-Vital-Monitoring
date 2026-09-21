"""Packages quality_rules.py so Dataflow workers can import it (passed via the setup_file option)."""
import setuptools

setuptools.setup(
    name="patient-vitals-quality-rules",
    version="1.0.0",
    py_modules=["quality_rules"],
)
