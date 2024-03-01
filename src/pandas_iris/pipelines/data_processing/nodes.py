"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.3
"""

import pandas as pd


def _is_true(x: pd.Series) -> pd.Series:
    return x == "t"


def _parse_percentage(x: pd.Series) -> pd.Series:
    x = x.str.replace("%", "")
    x = x.astype(float) / 100
    return x


def _parse_money(x: pd.Series) -> pd.Series:
    x = x.str.replace("$", "").str.replace(",", "")
    x = x.astype(float)
    return x


def preprocess_companies(companies: pd.DataFrame) -> pd.DataFrame:
    companies["iata_approved"] = _is_true(companies["iata_approved"])
    companies["company_rating"] = _parse_percentage(companies["company_rating"])
    return companies


def preprocess_shuttles(shuttles: pd.DataFrame) -> pd.DataFrame:
    shuttles["d_check_complete"] = _is_true(shuttles["d_check_complete"])
    shuttles["moon_clearance_complete"] = _is_true(shuttles["moon_clearance_complete"])
    shuttles["price"] = _parse_money(shuttles["price"])
    return shuttles

def create_model_input_table(companies: pd.DataFrame,
                             shuttles: pd.DataFrame,
                             reviews: pd.DataFrame) -> pd.DataFrame:
    # Merge suttle and reviews
    rated_shuttles = shuttles.merge(reviews, left_on="id", right_on="shuttle_id")
    # merge rated shuttles with company info
    model_input_table = rated_shuttles.merge(companies, left_on="company_id", right_on="id")
    return model_input_table.dropna()



