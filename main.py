import pandas as pd
import os
from dotenv import load_dotenv
import sqlalchemy as sqla
from sqlalchemy.engine import URL

load_dotenv()

DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USERNAME")
DB_PASS = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT")

url = URL.create(
    drivername="postgresql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    database=DB_NAME,
    port=DB_PORT,
)

engine = sqla.create_engine(url)

columns_eleicoes = [
    "SQ_CANDIDATO",
    "ANO_ELEICAO",
    "DS_ELEICAO",
    "DS_CARGO",
    "NM_CANDIDATO",
    "NM_PARTIDO",
    "DS_GENERO",
    "DS_GRAU_INSTRUCAO",
    "DS_OCUPACAO",
    "DS_COR_RACA",
    "SG_UF",
]
columns_bens = [
    "ANO_ELEICAO",
    "DS_ELEICAO",
    "SQ_CANDIDATO",
    "DS_TIPO_BEM_CANDIDATO",
    "VR_BEM_CANDIDATO",
]
transform_columns_eleicoes = [
    "DS_ELEICAO",
    "DS_CARGO",
    "NM_CANDIDATO",
    "DS_GENERO",
    "DS_GRAU_INSTRUCAO",
    "DS_OCUPACAO",
    "DS_COR_RACA",
]
renamed_columns_eleicoes = {
    "NM_CANDIDATO": "nome_canditato",
    "DS_ELEICAO": "eleicao",
    "SQ_CANDIDATO": "id_candidato",
    "ANO_ELEICAO": "ano_eleicao",
    "DS_CARGO": "cargo",
    "DS_GENERO": "genero",
    "DS_GRAU_INSTRUCAO": "grau_de_instrucao",
    "DS_COR_RACA": "cor_raca",
    "DS_OCUPACAO": "ocupacao",
    "NM_PARTIDO": "nome_partido",
    "SG_UF": "estado",
}
renamed_columns_bem = {
    "ANO_ELEICAO": "ano_eleicao",
    "DS_ELEICAO": "eleicao",
    "SQ_CANDIDATO": "id_candidato",
    "DS_TIPO_BEM_CANDIDATO": "tipo_bem",
    "VR_BEM_CANDIDATO": "valor_bem",
}
years = ["2012", "2014", "2016", "2018", "2020", "2022", "2024"]


def create_df_candidatos(columns: list, year: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            f"./inputs/consulta_cand_{year}_BRASIL.csv",
            sep=";",
            encoding="latin1",
            usecols=columns,
        )
    except:
        print("erro na leitura dos csv")

    return df


def create_df_bens(columns: list, year: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            f"./inputs/bem_candidato_{year}_BRASIL.csv",
            sep=";",
            encoding="latin1",
            usecols=columns,
        )
    except:
        print("erro na leitura dos csv")

    return df


def transform_text_df(
    df: pd.DataFrame, columns_to_transform: list
) -> pd.DataFrame:
    for column in columns_to_transform:
        df[column] = df[column].str.title()
    return df


def rename_columns(df: pd.DataFrame, columns_renamed: dict) -> pd.DataFrame:
    df.rename(columns=columns_renamed, inplace=True)

    return df


def df_to_database(db_table: str, df: pd.DataFrame, engine: sqla.Engine):
    df.to_sql(db_table, con=engine, if_exists="append", index=False)


def update_eleicoes():
    cont = 0
    for year in years:
        cont += 1
        print(f"{cont}/{len(years)} datasets eleicoes")
        df = create_df_candidatos(columns_eleicoes, year)
        df_transformed = transform_text_df(df, transform_columns_eleicoes)
        df_renamed = rename_columns(df_transformed, renamed_columns_eleicoes)
        df_renamed = df_renamed.reindex(
            [
                "id_candidato",
                "nome_canditato",
                "cargo",
                "nome_partido",
                "ano_eleicao",
                "eleicao",
                "estado",
                "genero",
                "grau_de_instrucao",
                "cor_raca",
                "ocupacao",
            ],
            axis=1,
        )
        df_to_database("eleicoes", df_renamed, engine)


def update_bem():
    cont = 0
    for year in years:
        cont += 1
        print(f"{cont}/{len(years)} datasets bens_candidatos")
        df = create_df_bens(columns_bens, year)
        df_renamed = rename_columns(df, renamed_columns_bem)
        df_renamed = df_renamed.reindex(
            ["id_candidato", "eleicao", "ano_eleicao", "tipo_bem", "valor_bem"],
            axis=1,
        )
        df_to_database('bem_candidato', df_renamed, engine)


def main():
    update_eleicoes()
    update_bem()

main()
