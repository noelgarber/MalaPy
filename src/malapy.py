import numpy as np
import pandas as pd
import requests
import time
import collections
from bs4 import BeautifulSoup

# Get the lists of diseases by category from MalaCards

def get_diseases_lists(categories = "default", output_type = "both", custom_urls = None):
    # Define the set of urls to use for category lookups
    if custom_urls == None:
        category_url_dict = {
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
    elif isinstance(custom_urls, dict):
        category_url_dict = custom_urls
    else:
        # custom_urls must be dict
        raise Exception("get_diseases_lists error: custom_urls must be None or <list>, not " + str(type(custom_urls)))

    # Check if a subset of categories should be used, or if all categories should be requested
    if categories == "default":
        category_url_subset = category_url_dict
    elif isinstance(categories, list):
        category_url_subset = {}
        for category in categories:
            category_url_subset[category] = category_url_dict.get(category)
    else:
        raise Exception("get_diseases_lists error: input was " + str(type(urls)) + ", but a dictionary was expected.")

    # Include a user agent header to prevent a 403: Forbidden error from MalaCards
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }

    disease_df_responses = {}
    disease_list_responses = {}

    for category, url in category_url_subset.items():
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

# Define a function that removes diseases that are not in a selected category

def drop_unselected_diseases(unfiltered_df, disease_lists_by_category, included_disease_categories = "All", excluded_disease_categories = "None"):
    # Check if unfiltered_df is a dataframe - this is required for the drop() method to work
    if not isinstance(unfiltered_df, pd.DataFrame):
        raise TypeError("drop_unselected_diseases() error: first positional argument should be input dataframe, but it is " + str(type(unfiltered_df)))

    # Ensure that filter criteria are correctly formatted
    if included_disease_categories == "All" and excluded_disease_categories == "None":
        # If no filter criteria are set, abort and return the original dataframe
        return unfiltered_df
    else:
        # Declare included disease categories
        if included_disease_categories == "All":
            # To include all disease categories, take all the keys from the dict of disease lists
            included_disease_categories = list(disease_lists_by_category.keys())
        elif isinstance(included_disease_categories, str):
            included_disease_categories = [included_disease_categories]
        elif not isinstance(included_disease_categories, list):
            raise TypeError("included_disease_categories in drop_unselected_diseases() must be string (single item) or list, not " + str(type(included_disease_categories)))
        elif len(included_disease_categories) == 0:
            # If no included disease categories are passed, use all of them
            included_disease_categories = list(disease_lists_by_category.keys())

        # Declare excluded disease categories
        if excluded_disease_categories == "None":
            excluded_disease_categories = []
        elif isinstance(excluded_disease_categories, str):
            excluded_disease_categories = [excluded_disease_categories]
        elif not isinstance(excluded_disease_categories, list):
            raise TypeError("excluded_disease_categories in drop_unselected_diseases() must be string (single item) or list, not " + str(type(excluded_disease_categories)))

    # Make a copy of the unfiltered dataframe; filtered rows will be dropped later
    filtered_df = unfiltered_df.copy()

    # Flatten included/excluded disease lists
    included_disease_lists = [disease_lists_by_category.get(category) for category in included_disease_categories]
    included_diseases = [disease for category_diseases in included_disease_lists for disease in category_diseases]

    excluded_disease_lists = [disease_lists_by_category.get(category) for category in excluded_disease_categories]
    excluded_diseases = [disease for category_diseases in excluded_disease_lists for disease in category_diseases]

    # Test if a disease is in an included category AND is not in an excluded category
    rows_to_drop = []
    for i in np.arange(len(unfiltered_df)):
        disease = unfiltered_df.at[i, "Name"]
        if disease not in included_diseases:
            rows_to_drop.append(i)
        elif disease in excluded_diseases:
            rows_to_drop.append(i)

    #Drop identified rows
    filtered_df = filtered_df.drop(index = rows_to_drop)

    return filtered_df


# Define a function that searches genes for disease associations and returns a list of associated diseases

def mala_checker(gene_name, elite_genes_only = False, output_type = "string", included_disease_categories = "All", excluded_disease_categories = "None", disease_list_responses = None, show_response_code = False):
    if disease_list_responses == None:
        print("No lists of diseases were given; pulling them from MalaCards...")
        disease_list_responses = get_diseases_lists(output_type="list")
        print("\tDone!")

    # Declare request URL based on whether all associated genes, or only Elite Genes (manually curated causal associations), should be searched for each disease association.
    if elite_genes_only:
        search_url = "https://www.malacards.org/search/results?query=%5BEL%5D+%28" + gene_name + "%29&pageSize=-1"
    else:
        search_url = "https://www.malacards.org/search/results?query=%5BGE%5D+%28" + gene_name + "%29&pageSize=-1"
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }
    print("Search url:", search_url)
    search_response = requests.get(search_url, headers = request_headers)
    print("\tResponse code:", search_response.status_code) if show_response_code else None

    search_soup = BeautifulSoup(search_response.content, "html.parser")
    tables = search_soup.find_all("table")

    # Check if tables containing the data are present
    if tables == None or len(tables) < 2:
        # Abort early because no data was found in the search; return empty values
        if output_type == "string":
            return 0, ""
        elif output_type == "list":
            return 0, []
        elif output_type == "df" or output_type == "dataframe":
            return 0, pd.DataFrame()

    # Take the second table, which contains the disease list data
    search_table = tables[1]
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

    results_df = drop_unselected_diseases(unfiltered_df = results_df, disease_lists_by_category = disease_list_responses,
                                          included_disease_categories = included_disease_categories, excluded_disease_categories = excluded_disease_categories)

    results_list = results_df["Name"].values.tolist()
    results_count = len(results_list)
    print("Count of results:", str(results_count))

    results_string = "; ".join(str(x) for x in results_list)

    if output_type == "string":
        return results_count, results_string
    elif output_type == "list":
        return results_count, results_list
    elif output_type == "df" or output_type == "dataframe":
        return results_count, results_df

def check_gene_list(gene_list, elite_genes_only = False, output_type = "dict", included_disease_categories = "All", excluded_disease_categories = "All", disease_list_responses = None, show_response_codes = False):
    if disease_list_responses == None:
        print("No lists of diseases were given; pulling them from MalaCards...")

        # If included/excluded disease categories are specified, collect the total list of relevant categories to pull from MalaCards
        relevant_disease_categories = []
        if included_disease_categories != "All":
            relevant_disease_categories.extend(included_disease_categories)
        if excluded_disease_categories != "All":
            relevant_disease_categories.extend(excluded_disease_categories)

        # If included/excluded disease categories are specified, request only these categories from MalaCards; otherwise, request all of them
        if len(relevant_disease_categories) > 0:
            disease_list_responses = get_diseases_lists(categories = relevant_disease_categories, output_type="list")
        else:
            disease_list_responses = get_diseases_lists(output_type="list")

    if disease_list_responses == None:
        print("No lists of diseases were given; pulling them from MalaCards...")
        disease_list_responses = get_diseases_lists(output_type = "list")
        print("\tDone!")

    # Set the delay for making MalaCards requests to prevent HTTP 429 (too many requests)
    request_delay = 0.5

    if output_type == "dict":
        gene_mala_dict = {}
        for gene in gene_list:
            results_count, results_list = mala_checker(gene, elite_genes_only = elite_genes_only, output_type = "list", included_disease_categories = included_disease_categories,
                                                  excluded_disease_categories = excluded_disease_categories, disease_list_responses = disease_list_responses,
                                                  show_response_code = show_response_codes)
            gene_mala_dict[gene] = (results_count, results_list)
            time.sleep(request_delay)
        return gene_mala_dict
    elif output_type == "df" or output_type == "dataframe":
        gene_mala_df = pd.DataFrame(columns = ["Gene", "Results_Count", "Results_List"])
        for i, gene in enumerate(gene_list):
            results_count, results_string = mala_checker(gene, elite_genes_only = elite_genes_only, output_type = "string", included_disease_categories = included_disease_categories,
                                                  excluded_disease_categories = excluded_disease_categories, disease_list_responses = disease_list_responses,
                                                  show_response_code = show_response_codes)
            new_row = gene, results_count, results_string
            gene_mala_df = gene_mala_df.append(pd.Series(dict(zip(gene_mala_df.columns, new_row))), ignore_index=True)
            time.sleep(request_delay)
        return gene_mala_df
    else:
        raise Exception("unsupported output_type in check_gene_list(): expected \"dict\", \"df\", or \"dataframe\", but got " + str(output_type))

# Execute main business logic if malapy.py is being run as a standalone script
if __name__ == "__main__":
    # prompt the user to declare whether a single gene or a whole list should be queried
    type_declared = False
    while not type_declared:
        input_type = input("Do you want to check a single gene or a whole list? Please enter \"gene\" or \"list\":  ")
        if input_type == "gene" or input_type == "list":
            type_declared = True
        else:
            print("Unsupported input. Please try again.")

    # prompt to define inclusions and exclusions
    print("Please input the list of disease categories to INCLUDE in the search, or hit enter to use the default set.")
    done_included_diseases = False
    included_diseases = []
    while not done_included_diseases:
        included_disease = input("\tEnter included disease category:  ")
        if included_disease == "":
            done_included_diseases = True
        else:
            included_diseases.append(included_disease)
    print("Please input the list of disease categories to EXCLUDE in the search (matching entries are always rejected), or hit enter to specify none.")
    done_excluded_diseases = False
    excluded_diseases = []
    while not done_excluded_diseases:
        excluded_disease = input("\tEnter excluded disease category:  ")
        if excluded_disease == "":
            done_excluded_diseases = True
        else:
            excluded_diseases.append(excluded_disease)
    elite_genes_only = input("Query elite genes (causal, manually curated associations) only? (Y/N)  ")

    #obtain the gene(s) to search
    if input_type == "list":
        gene_list_path = input("Enter the path to a CSV file containing the list of genes:  ")
        gene_df = pd.read_csv(gene_list_path)
        gene_list_header = input("Enter the column name containing the gene list, or hit enter to use the first column:  ")
        if gene_list_header == "":
            gene_list = gene_df.iloc[:,0].tolist()
        else:
            gene_list = gene_df[gene_list_header].values.tolist()

        if elite_genes_only == "Y":
            results_df = check_gene_list(gene_list = gene_list, elite_genes_only = True, output_type = "df", included_disease_categories = included_diseases,
                                         excluded_disease_categories = excluded_diseases, show_response_codes = True)
        else:
            results_df = check_gene_list(gene_list = gene_list, elite_genes_only = False, output_type = "df", included_disease_categories = included_diseases,
                                         excluded_disease_categories = excluded_diseases, show_response_codes = True)

        # Construct output filename/path
        included_diseases_str = "including"
        for included_disease in included_diseases:
            included_diseases_str = included_diseases_str + "-" + included_disease
        excluded_diseases_str = "excluding"
        for excluded_disease in excluded_diseases:
            excluded_diseases_str = excluded_diseases_str + "-" + excluded_disease
        output_path = gene_list_path[:-4] + "_results_" + included_diseases_str + "_" + excluded_diseases_str + ".csv"

        # Save output file
        results_df.to_csv(output_path)

    elif input_type == "gene":
        gene_name = input("Enter the gene name to search:  ")
        results_tuple = mala_checker()

