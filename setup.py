from setuptools import setup, find_packages

setup(
    name="schwab-tracker",
    version="0.9.2",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    setup_requires=['setuptools'],
    install_requires=[
        "pyyaml>=6.0",
        "python-dotenv>=1.0.0",
        "schwabdev",
        "requests",  # Added for AlphaVantage API
    ],
    entry_points={
        'console_scripts': [
            'analyze-options=schwab_tracker.scripts.analyze_options:main',
            'collect-data=schwab_tracker.scripts.collect_data:main',
            'get-symbols=schwab_tracker.scripts.get_active_symbols:main',
        ],
    },
    python_requires=">=3.7",
    author="Erik Zolan",
    author_email="erik.zolan@gmail.com",
    description="A tool for analyzing Schwab API data and options",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
)