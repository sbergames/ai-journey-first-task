from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).parents[1]


def sql_download(sql: str, table_name: str, save_dir: Path, drop_columns: list = None) -> None:
    df = pd.read_gbq(sql, project_id='prostokvashino-m3', progress_bar_type='tqdm')
    if drop_columns:
        df = df.drop(columns=drop_columns, errors='ignore')
    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = save_dir / f"{table_name}.csv"
    df.to_csv(save_path)


def load_data(start_date: str, end_date: str, save_dir: Path) -> None:
    tables = [
        'prod_mt.MT_click_button',
        'prod_mt.MT_level_end',
        'prod_mt.MT_level_start',
        'prod_mt.MT_math_exercise',
        'prod_mt.MT_session_end',
        'prod_mt.MT_show_windows',
    ]

    sql_agg = f"""
        SELECT *
        FROM agg_data.agg_session agg
        WHERE (agg.install_datetime >= '{start_date}')
          AND (agg.install_datetime < '{end_date}')
          AND (agg.session_time != 0)
    """
    sql_download(sql_agg, 'agg_data.agg_session', save_dir, drop_columns=['device_OS', 'device_id', 'install_id', 'install_datetime'])

    for table in tables:
        sql = f"""
        WITH sessions AS ({sql_agg})

        SELECT *
        FROM {table} t
        WHERE t.session_id in (SELECT DISTINCT session_id FROM sessions)
        """
        sql_download(sql, table, save_dir, drop_columns=['user_id', 'device_OS', 'device_id', 'install_id', 'send_age'])


if __name__ == "__main__":
    # load_data(start_date='2022-02-01', end_date='2022-03-01', save_dir=BASE_DIR / 'files' / 'public')
    # load_data(start_date='2022-03-01', end_date='2022-03-15', save_dir=BASE_DIR / 'files' / 'private')

    paths = [BASE_DIR / 'files' / folder / 'agg_data.agg_session.csv' for folder in ['private', 'public']]

    for agg_path in paths:
        agg_session = pd.read_csv(agg_path, index_col=0).drop(columns=['event_date', 'process_date', 'app_bundle_id'], errors='ignore')
        agg_session = agg_session.drop(columns=['event_date', 'process_date', 'app_bundle_id'], errors='ignore')
        agg_session = agg_session.sort_values(['user_id', 'start_session'])
        target = (agg_session['user_id'] == agg_session['user_id'].shift(-1)).astype(int)
        agg_session = agg_session.assign(target=target).drop(columns=['user_id'])
        agg_session = agg_session.sample(frac=1, random_state=42)
        agg_session.to_csv(agg_path)
