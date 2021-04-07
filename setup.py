from setuptools import setup


package_data = {'audb': ['core/etc/*']}

setup(
    use_scm_version=True,
    package_data=package_data
)
