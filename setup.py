from setuptools import setup, find_namespace_packages
from pathlib    import Path

base_dir     = Path(__file__).parent.resolve()
version_file = base_dir / "lib/seattleflu/id3c/__version__.py"
readme_file  = base_dir / "README.md"

# Eval the version file to get __version__; avoids importing our own package
with version_file.open() as f:
    exec(f.read())

# Get the long description from the README file
with readme_file.open(encoding = "utf-8") as f:
    long_description = f.read()

setup(
    name = "seattleflu-id3c-customizations",
    version = __version__,

    packages = find_namespace_packages("lib"),
    package_dir = {"": "lib"},

    description = "Seattle Flu Study ID3C customizations",
    long_description = long_description,
    long_description_content_type = "text/markdown",

    url = "https://github.com/seattleflu/id3c-customizations",
    project_urls = {
        "Bug Reports": "https://github.com/seattleflu/id3c-customizations/issues",
        "Source":      "https://github.com/seattleflu/id3c-customizations",
    },

    classifiers = [
        "Development Status :: 5 - Production/Stable",

        # This is for bioinformatic software devs and researchers
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",

        # Python ≥ 3.6 only
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],

    # Register extended ID3C CLI commands and API routes
    entry_points = {
        "id3c.cli.commands": [
            "seattleflu = seattleflu.id3c.cli.command"
        ],
        "id3c.cli.command.etl.fhir": [
            "https://seattleflu.org = seattleflu.id3c.cli.command.etl.fhir_customizations"
        ],
        "id3c.api.routes": [
            "seattleflu = seattleflu.id3c.api.routes"
        ],
    },

    python_requires = ">=3.6",

    install_requires = [
        "id3c >=2020.2",
        "click >=7.0",
        "regex",
        "requests",
        "flask",
        "flask-cors",
    ],

    extras_require = {
        "locations": [
            "pandas >=1.0.1,<2",
            "snakemake",
        ],
    },
)
