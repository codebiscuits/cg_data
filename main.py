from pycoingecko import CoinGeckoAPI
import statistics as stats
from datetime import datetime
from pathlib import Path
from pprint import pprint
import json
import pandas as pd
from datetime import datetime, timezone
import time
from pushbullet import Pushbullet

cg = CoinGeckoAPI()
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
# pb = Pushbullet('o.H4ZkitbaJgqx9vxo5kL2MMwnlANcloxT')

folder = Path('/home/ross/coding/cg_data/cg_data')
folder.mkdir(exist_ok=True)

def top_300_returns():
    coin_list = cg.get_coins_markets(vs_currency='usd', per_page=150, price_change_percentage='7d')
    coin_list_2 = cg.get_coins_markets(vs_currency='usd', per_page=150, page=2, price_change_percentage='7d')
    coin_list.extend(coin_list_2)

    change_24h = []
    change_7d = []

    for i in coin_list:
        d1 = i.get('price_change_percentage_24h')
        if type(d1) == float:
            change_24h.append(d1)
        else:
            change_24h.append(0)

        d7 = i.get('price_change_percentage_7d_in_currency')
        if type(d7) == float:
            change_7d.append(d7)
        else:
            change_7d.append(0)

    return change_24h, change_7d


def mcap_stats(change_24h, save):
    mcap_row = dict(
        date=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M'),
        avg_300=stats.mean(change_24h),
        std_300=stats.stdev(change_24h),
        avg_50=stats.mean(change_24h[:50]),
        std_50=stats.stdev(change_24h[:50]),
        avg_51_150=stats.mean(change_24h[51:150]),
        std_51_150=stats.stdev(change_24h[51:150]),
        avg_151_300=stats.mean(change_24h[151:]),
        std_151_300=stats.stdev(change_24h[151:])
    )
    mcap_new = pd.DataFrame(data=mcap_row, index=[0], columns=mcap_row.keys())

    mcap_df = pd.read_parquet(folder / 'mcap_roc.parquet')
    mcap_df = pd.concat([mcap_df, mcap_new], ignore_index=True)

    if save:
        mcap_df.to_parquet(folder / 'mcap_roc.parquet')


def indiv_stats(change_7d, save):
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
    change_data = dict(enumerate(change_7d))
    change_dict = {str(k+1): v for k, v in change_data.items()}
    indiv_row = {'date': now} | change_dict
    indiv_new = pd.DataFrame(data=indiv_row, index=[0], columns=[str(k) for k in indiv_row.keys()])

    indiv_df = pd.read_parquet(folder / 'indiv_roc.parquet')
    indiv_df = pd.concat([indiv_df, indiv_new], ignore_index=True)

    if save:
        indiv_df.to_parquet(folder / 'indiv_roc.parquet')


def category_strength(save):
    cat_file = folder / f"category_strength_{datetime.now().year}.json"
    cat_file.touch(exist_ok=True)
    try:
        with open(cat_file, 'r') as categories:
            cat_data = json.load(categories)
    except json.decoder.JSONDecodeError:
        cat_data = {}

    index_list = cg.get_coins_categories()
    data = {}
    for i in index_list:
        id = i['id']
        cap = i['market_cap']
        vol = i['volume_24h']
        ret = i['market_cap_change_24h']
        data[id] = {'mcap': cap, 'volume_24h': vol, 'return_24h': ret}

    dt = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
    cat_data[dt] = data

    if save:
        with open(cat_file, 'w') as categories:
            json.dump(cat_data, categories)


def whole_market(save):
    global_data = cg.get_global()
    total_cap = global_data['total_market_cap']['usd']
    total_vol = global_data['total_volume']['usd']
    top_ten_dom = global_data['market_cap_percentage']
    btc_dom = top_ten_dom['btc']
    eth_dom = top_ten_dom['eth']
    usdt_dom = top_ten_dom.get('usdt', 0)
    usdc_dom = top_ten_dom.get('usdc', 0)

    dt = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
    global_row = pd.DataFrame({'date': dt, 'total_cap': total_cap, 'total_vol': total_vol,
                  'btc_dom': btc_dom, 'eth_dom': eth_dom,
                  'stable_dom': usdt_dom + usdc_dom}, index=[0])


    global_file = folder / f"whole_market_data_{datetime.now().year}.parquet"

    if global_file.exists():
        old_data = pd.read_parquet(global_file)
    else:
        old_data = pd.DataFrame(columns=global_row.columns)

    df = pd.concat([old_data, global_row], ignore_index=True)

    if save:
        df.to_parquet(global_file)


if __name__ == '__main__':
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    print(f"{now} Running Coingecko data collection")
    all_start = time.perf_counter()
    live = True
    try:
        change_24h, change_7d = top_300_returns()
        mcap_stats(change_24h, save=live)
        indiv_stats(change_7d, save=live)
        category_strength(save=live)
        whole_market(save=live)
    except (ValueError, TypeError, KeyError) as e:
        print('Error during data collection')
        print(e)
        # pb.push_note(now, 'Coingecko data collection had a problem')


    all_end = time.perf_counter()
    elapsed = all_end - all_start
    print(f"Data collection complete, total time taken: {int(elapsed // 60)}m {elapsed % 60:.1f}s")
