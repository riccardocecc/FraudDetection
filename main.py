from UploadDataset import load_data
from GenDataset import generate_and_save_datasets
from Query import run_all_queries
if __name__ == "__main__":

    #generate_and_save_datasets()

    folder_size = "50MB"  #  con la dimensione del file che vuoi caricare (50MB, 100MB, 200MB)

    #load_data(folder_size)

    run_all_queries(folder_size)


