from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="scope3track",
    version="1.0.0",
    author="",
    description="Carbon and Scope 3 emissions tracking for SMBs — GHG Protocol compliant calculations, supplier tracking, CSRD-ready reporting",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/scope3track-py/scope3track",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.8",
    install_requires=[
        "pydantic>=2.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ],
    keywords=[
        "scope 3 emissions", "carbon tracking python", "GHG protocol",
        "CSRD compliance", "carbon footprint calculator",
        "scope3 reporting", "sustainability reporting",
        "carbon accounting SMB", "emission factor calculation",
        "supply chain emissions tracking",
    ],
)
