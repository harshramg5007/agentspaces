"""Setup script for the launch-supported Agent Spaces Python SDK."""

from pathlib import Path

from setuptools import find_packages, setup


README = Path(__file__).with_name("README.md").read_text(encoding="utf-8")


setup(
    name="agent-space-sdk",
    version="1.0.0",
    author="Urobora Inc",
    description="Python SDK for the Agent Spaces coordination runtime",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/urobora-ai/agentspaces",
    project_urls={
        "Documentation": "https://github.com/urobora-ai/agentspaces/tree/main/docs",
        "Source": "https://github.com/urobora-ai/agentspaces",
        "License": "https://www.gnu.org/licenses/agpl-3.0.html",
    },
    license="AGPL-3.0-only",
    license_files=["LICENSE"],
    packages=find_packages(
        include=[
            "agent_space_sdk",
            "agent_space_sdk.*",
        ]
    ),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
    ],
    keywords=[
        "agents",
        "coordination",
        "multi-agent",
        "task-queue",
        "inference",
    ],
    python_requires=">=3.11",
    install_requires=[
        "requests>=2.28.0",
    ],
    extras_require={
        "async": [
            "aiohttp>=3.8.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "build>=1.2.2",
            "twine>=5.0.0",
        ],
        "analytics": [
            "psycopg[binary]>=3.1.0",
        ],
    },
)
