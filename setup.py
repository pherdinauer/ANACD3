"""Setup script for ANAC Sync."""

from setuptools import setup, find_packages

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="anacsync",
    version="0.1.0",
    author="ANAC Sync Team",
    author_email="contact@example.com",
    description="Professional ANAC dataset crawler and downloader with multi-strategy download system",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/example/anacsync",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "Topic :: System :: Archiving",
        "Topic :: Utilities",
    ],
    python_requires=">=3.11",
    install_requires=[
        "httpx>=0.25.0",
        "typer[all]>=0.9.0",
        "rich>=13.0.0",
        "pydantic>=2.0.0",
        "tenacity>=8.0.0",
        "python-dotenv>=1.0.0",
        "selectolax>=0.3.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0.0",
            "ruff>=0.1.0",
            "black>=23.0.0",
            "mypy>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "anacsync=anacsync.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)

