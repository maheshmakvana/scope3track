from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="scope3track",
    version="1.2.1",
    author="",
    description="Carbon and Scope 3 emissions tracking — GHG Protocol, emission hotspot analysis, Net Zero roadmap generation, SBTi alignment, CSRD-ready reporting",
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
        "supply chain emissions tracking", "net zero roadmap python",
        "emission hotspot analysis", "SBTi alignment checker python",
        "Science Based Targets python", "carbon reduction scenarios",
        "net zero planning tool python", "CSRD reporting tool",
    ],
)
