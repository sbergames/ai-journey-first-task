from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).parents[1]


def sql_download(sql: str, table_name) -> None:
    df = pd.read_gbq(sql, project_id='prostokvashino-m3', progress_bar_type='tqdm')
    df = df.drop(columns=['device_OS', 'device_id'], errors='ignore')
    save_path = BASE_DIR / 'data' / f"{table_name}.csv"
    df.to_csv(save_path)


def load_data() -> None:
    tables = [
        'prod_mt.MT_click_button',
        'prod_mt.MT_level_end',
        'prod_mt.MT_level_start',
        'prod_mt.MT_math_exercise',
        'prod_mt.MT_session_end',
        'prod_mt.MT_show_windows',
    ]

    sql_agg = """
        SELECT *
        FROM agg_data.agg_session agg
        WHERE agg.install_datetime >= '2022-02-01' and agg.install_datetime < '2022-03-01'
    """
    sql_download(sql_agg, 'agg_data.agg_session')

    for table in tables:
        sql = f"""
        WITH sessions AS ({sql_agg})

        SELECT *
        FROM {table} t
        WHERE t.session_id in (SELECT DISTINCT session_id FROM sessions)
        """
        sql_download(sql, table)


if __name__ == "__main__":
    load_data()
