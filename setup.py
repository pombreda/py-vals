from setuptools import setup
    
setup(
    name = "vals",
    py_modules = ["vals"],
    scripts = ["vals.py"],
    version = "0.0.1",
    license = "LGPL",
    zip_safe=False,
    install_requires=["baker"],
    description = "value-stream processing",
    author = "karasuyamatengu",
    author_email = "karasuyamatengu@gmail.com",
    url = "https://github.com/tengu/vals",
    keywords = [],
    classifiers = [],
    long_description = """A collection of commands to aid value-stream processing. 
Value stream means generalization of unix text processing to json stream.
jq (http://stedolan.github.io/jq/) is sed+awk in this context.
vals.py is where I throw in everything that jq cannot handle.
""",
)
