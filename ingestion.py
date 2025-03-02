from pathlib import Path
import urllib.request
import zipfile

from metaflow import FlowSpec, Parameter, step


class IngestionFlow(FlowSpec):
    DATASET_URL = "https://www.kaggle.com/api/v1/datasets/download/ransakaravihara/anime-recommendation-ltr-dataset"
    save_path = Parameter(
        "save_path",
        help="Directory to save the dataset",
        default="./data/anime-recommendation-ltr-dataset.zip",
    )
    unzip = Parameter("unzip", help="", default=True)
    remove_zip = Parameter(
        "remove_zip", help="Remove the zip file after unzipping", default=True
    )

    @step
    def start(self):
        self.file_path = Path(self.save_path).resolve()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.next(self.download)

    @step
    def download(self):
        urllib.request.urlretrieve(self.DATASET_URL, self.file_path)
        self.next(self.unzip_files)

    @step
    def unzip_files(self):
        if self.unzip_files:
            with zipfile.ZipFile(self.file_path, "r") as zip_ref:
                zip_ref.extractall(self.file_path.parent)
            if self.remove_zip:
                self.file_path.unlink()
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    IngestionFlow()
