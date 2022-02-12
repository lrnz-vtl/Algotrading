import setuptools

setuptools.setup(name='algo',
                 install_requires=['ts_tools_algo'],
                 dependency_links=[
                        'git+ssh://git@github.com/lrnz-vtl/ts_tools_algo.git#egg=0.6.0'
                        ],
                 version='0.1.0',
                 packages=setuptools.find_packages(include=["algo", "algo.*"])
                 )
