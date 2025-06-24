import setuptools

setuptools.setup(
    name="mixpanel_gcs_importer",
    version="18.0.0",
    description="A Dataflow pipeline to import GCS data into Mixpanel.",
    install_requires=[
        "apache-beam[gcp]==2.57.0",
        "requests>=2.31.0,<3.0.0",
        "python-dateutil>=2.8.2,<3.0.0",
        "google-cloud-storage>=2.5.0,<3.0.0",
        "pyarrow>=10.0.0,<17.0.0",
    ],
    py_modules=["main", "main_batch", "gcs_utils"],
)
