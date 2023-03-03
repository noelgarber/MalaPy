import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Get the lists of diseases by category from MalaCards

def get_diseases_lists(urls = "default", output_type = "both"):
    if urls == "default":
        disease_urls = {
            "Blood": "https://www.malacards.org/categories/blood_disease_list",
            "Bone": "https://www.malacards.org/categories/bone_disease_list",
            "Cardiovascular": "https://www.malacards.org/categories/cardiovascular_disease_list",
            "Ear": "https://www.malacards.org/categories/ear_disease_list",
            "Endocrine": "https://www.malacards.org/categories/endocrine_disease_list",
            "Eye": "https://www.malacards.org/categories/eye_disease_list",
            "Gastrointestinal": "https://www.malacards.org/categories/gastrointestinal_disease_list",
            "Immune": "https://www.malacards.org/categories/immune_disease_list",
            "Liver": "https://www.malacards.org/categories/liver_disease_list",
            "Mental": "https://www.malacards.org/categories/mental_disease_list",
            "Muscle": "https://www.malacards.org/categories/muscle_disease_list",
            "Nephrological": "https://www.malacards.org/categories/nephrological_disease_list",
            "Neuronal": "https://www.malacards.org/categories/neuronal_disease_list",
            "Oral": "https://www.malacards.org/categories/oral_disease_list",
            "Reproductive": "https://www.malacards.org/categories/reproductive_disease_list",
            "Respiratory": "https://www.malacards.org/categories/respiratory_disease_list",
            "Skin": "https://www.malacards.org/categories/skin_disease_list",
            "Smell/Taste": "https://www.malacards.org/categories/smell_taste_disease_list",
            "Cancer": "https://www.malacards.org/categories/cancer_disease_list",
            "Fetal": "https://www.malacards.org/categories/fetal_disease_list",
            "Genetic": "https://www.malacards.org/categories/genetic_disease_list",
            "Infectious": "https://www.malacards.org/categories/infectious_disease_list",
            "Metabolic": "https://www.malacards.org/categories/metabolic_disease_list",
            "Rare": "https://www.malacards.org/categories/rare_diseases"
        }
    elif isinstance(urls, dict):
        disease_urls = urls
    else:
        raise Exception("get_diseases_lists error: input was " + str(type(urls)) + ", but a dictionary was expected.")

    # Include a user agent header to prevent a 403: Forbidden error from MalaCards
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }

    disease_df_responses = {}
    disease_list_responses = {}

    for category, url in disease_urls.items():
        response = requests.get(url, headers = request_headers)

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")
        data = []
        for row in rows:
            cols = row.find_all("td")
            cols = [col.text.strip() for col in cols]
            data.append(cols)

        disease_cols = ["#", "Family", "MCID", "Name", "MIFTS"]
        diseases_df = pd.DataFrame(data[1:], columns = disease_cols)
        diseases_list = diseases_df["Name"].values.tolist()

        disease_df_responses[category] = diseases_df
        disease_list_responses[category] = diseases_list

    if output_type == "both":
        return disease_df_responses, disease_list_responses
    elif output_type == "list":
        return disease_list_responses
    elif output_type == "df" or output_type == "dataframe":
        return disease_df_responses
    else:
        raise Exception("get_diseases_lists error: unexpected output type (" + str(output_type) + ") was selected.")

# Define a function that searches genes for disease associations and returns a list of associated diseases

def mala_checker(protein_name, output_type = "string", disease_filter = "All", disease_list_responses = None, show_response_code = False):
    if disease_list_responses == None:
        print("No lists of diseases were given; pulling them from MalaCards...")
        disease_list_responses = get_diseases_lists(output_type="list")
        print("\tDone!")

    search_url = "https://www.malacards.org/search/results?query=%5BGE%5D+%28" + protein_name + "%29&pageSize=-1"
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }
    search_response = requests.get(search_url, headers = request_headers)
    print("\tResponse code:", search_response.status_code) if show_response_code else None

    search_soup = BeautifulSoup(search_response.content, "html.parser")
    tables = search_soup.find_all("table")
    if tables == None:
        return ""
    try:
        search_table = tables[1] # take the second table, which contains all the disease list data
    except IndexError:
        return ""
    search_rows = search_table.find_all("tr")

    # Extract the data
    search_data = []
    for row in search_rows:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        search_data.append(cols)

    # Even rows are blank; take only the odd rows
    search_data_odd = []
    for i, row in enumerate(search_data):
        if i % 2 != 0:
            search_data_odd.append(row)

    search_cols = ["#", "Blank", "Family", "MCID", "Name", "MIFTS", "Score"]
    results_df = pd.DataFrame(search_data_odd, columns = search_cols)
    results_df = results_df.drop("Blank", axis = 1)

    if disease_filter != "All":
        rows_to_drop = []
        diseases_list = disease_list_responses.get(disease_filter)
        for i in np.arange(len(results_df)):
            name = results_df.at[i, "Name"]
            if name not in diseases_list:
                rows_to_drop.append(i)
        results_df = results_df.drop(index = rows_to_drop)

    results_list = results_df["Name"].values.tolist()

    results_count = len(results_list)
    results_string = ", ".join(str(x) for x in results_list)

    if output_type == "string":
        return results_count, results_string
    elif output_type == "list":
        return results_count, results_list
    elif output_type == "df" or output_type == "dataframe":
        return results_count, results_df

def check_gene_list(gene_list, entry_output_type = "string", disease_filter = "All", disease_list_responses = None, show_response_codes = False):
    if disease_list_responses == None:
        print("No lists of diseases were given; pulling them from MalaCards...")
        disease_list_responses = get_diseases_lists(output_type = "list")
        print("\tDone!")

    gene_mala_dict = {}
    for gene in gene_list:
        results_count, results = mala_checker(gene, output_type = entry_output_type, disease_filter = disease_filter, disease_list_responses = disease_list_responses, show_response_code = show_response_codes)
        gene_mala_dict[gene] = (results_count, results)
    return gene_mala_dict
