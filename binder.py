from dataclasses import dataclass
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Optional, Type

import pandas as pd
import requests
import yaml
from rich import box
from rich.console import Console, JustifyMethod
from rich.table import Table

COLLECTION_URL = "https://api.cardtrader.com/api/v2/products/export"
BLUEPRINT_URL = "https://api.cardtrader.com/api/v2/blueprints/"
EXPANSIONS_URL = "https://api.cardtrader.com/api/v2/expansions"

pd.set_option('mode.chained_assignment',None)

class Color(Enum):
    MULTICOLOR = ("M", "dark_goldenrod")
    WHITE = ("W", "wheat1")
    BLUE = ("U", "blue")
    BLACK = ("B", "purple")
    RED = ("R", "red")
    GREEN = ("G", "green")
    ARTIFACTS = ("C", "orange4")
    LANDS = ("L", "steel_blue")

    def __init__(self, code: str, format: str) -> None:
        self.code = code
        self.format = format

    @classmethod
    def all(cls: Type["Color"], exclude_multi: bool = False) -> list[str]:
        return [
            color.code
            for color in list(cls)
            if not (exclude_multi and color is cls.MULTICOLOR)
        ]


@dataclass(frozen=True)
class Config:
    ct_token: str
    price_cents_threshold: int
    colors: list[Color]


def get_config(path: Path = Path("config.yaml")) -> Config:
    with open(path) as file:
        config_dict = yaml.load(file, Loader=yaml.FullLoader)
    config_dict["colors"] = [
        color for color in list(Color) if color.code in config_dict["colors"]
    ]
    config = Config(**config_dict)
    return config


def expansions_dict(
    expansions: list[dict], game_id: int = 1
) -> dict[str, dict[str, str]]:
    return {
        exp["id"]: {"name": exp["name"], "code": exp["code"]}
        for exp in expansions
        if exp["game_id"] == game_id
    }


def get_expansion(
    blueprint_id: int, ct_token: str, expansions: dict[str, dict[str, str]]
) -> str:
    header = {"Authorization": f"Bearer {ct_token}"}
    resp = requests.get(url=BLUEPRINT_URL + str(blueprint_id), headers=header)
    resp.raise_for_status()
    return expansions[resp.json()["expansion_id"]]["name"]


def df_to_table(
    pandas_dataframe: pd.DataFrame,
    rich_table: Table,
    show_index: bool = True,
    index_name: Optional[str] = None,
    formats: Optional[list[tuple[Optional[str], JustifyMethod]]] = None,
) -> Table:
    if show_index:
        index_name = str(index_name) if index_name else ""
        rich_table.add_column(index_name)

    for i, column in enumerate(pandas_dataframe.columns):
        style, justify = formats[i] if formats else (None, "left")
        rich_table.add_column(str(column), style=style, justify=justify)

    for index, value_list in enumerate(pandas_dataframe.values.tolist()):
        row = [str(index)] if show_index else []
        row += [str(x) for x in value_list]
        rich_table.add_row(*row)

    return rich_table


def main() -> None:
    console = Console()
    config = get_config()
    price_thresh = config.price_cents_threshold
    colors = config.colors
    columns = {
        "quantity": (None, "right"),
        "name": (None, "left"),
        "expansion": (None, "left"),
        "price": (None, "right"),
        "language": (None, "center"),
        "foil": (None, "center"),
    }

    header = {"Authorization": f"Bearer {config.ct_token}"}
    resp = requests.get(url=EXPANSIONS_URL, headers=header)
    resp.raise_for_status()
    exp_dict = expansions_dict(resp.json())
    get_expansion_f = partial(
        get_expansion, ct_token=config.ct_token, expansions=exp_dict
    )

    resp = requests.get(url=COLLECTION_URL, headers=header)
    resp.raise_for_status()
    data: list = resp.json()

    print(f"> Reading collection from CardTrader: {len(data)} items")

    data_pd = (
        pd.json_normalize(data)
        .sort_values("price_cents", ascending=False)
        .dropna(subset=["properties_hash.mtg_card_colors"])
        .reset_index()
    )
    data_pd.rename(
        columns={
            column: column.replace("properties_hash.", "")
            for column in data_pd.columns
        },
        inplace=True,
    )
    data_pd.rename(
        columns={"mtg_language": "language", "name_en": "name"}, inplace=True
    )
    data_pd["price"] = data_pd["price_cents"].apply(
        (lambda x: f"{x/100:.2f} €")
    )
    data_pd["foil"] = data_pd["mtg_foil"].replace(
        [True, False], [":heavy_check_mark:", ""]
    )
    data_pd["mtg_card_colors"] = data_pd["mtg_card_colors"].str.upper()

    total_items = 0
    total_value = 0.0
    for color in colors:
        if color is not Color.MULTICOLOR:
            query = data_pd.query(
                "`mtg_card_colors` == @color.code and "
                "`price_cents` >= @price_thresh"
            )
        else:
            color_codes = Color.all(exclude_multi=True)
            query = data_pd.query(
                "`mtg_card_colors` not in @color_codes and "
                "`price_cents` >= @price_thresh"
            )
        query["expansion"] = query["blueprint_id"].apply(get_expansion_f)
        table = Table(
            row_styles=["", "dim"],
            caption_justify="right",
            title_style=f"bold {color.format}",
            box=box.ROUNDED,
            border_style=f"{color.format}",
            width=150,
        )
        table = df_to_table(
            query[list(columns.keys())],
            table,
            show_index=False,
            formats=list(columns.values()),
        )
        table.title = f"{color.name} above {price_thresh/100.0:.2f} €"
        table.caption = (
            f"{len(query)} items ({query['price_cents'].sum()/100:.2f} €)"
        )
        console.print(table)
        total_items += len(query)
        total_value += query["price_cents"].sum() / 100

    print(
        "*** Total number of items in the binder: "
        f"{total_items} ({total_value:.2f} €)***"
    )


if __name__ == "__main__":
    main()
