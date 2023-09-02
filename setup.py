from setuptools import setup

setup(
    name="nano-to-sql",
    version="1.0",
    py_modules=["nano_to_sql", "known_accounts_to_sql"],
    install_requires=[],
    entry_points={
        "console_scripts": [
            "nano-to-sql=nano_to_sql:main",
            "known-accounts-to-sql=known_accounts_to_sql:main",
        ],
    },
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.6",
)
