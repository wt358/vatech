import argparse
import pandas as pd
import plotly.express as px
import plotly.io as po


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="input path", default="parquet")
    parser.add_argument("-e", "--engine", help="parquet engine", default="pyarrow")
    parser.add_argument("-t", "--target", help="target plot", default="count")
    args = parser.parse_args()

    df = pd.read_parquet(args.input, engine=args.engine)
    print(df)

    xtitle = "treatments"
    ytitle = ""
    yfield = ""

    if args.target == "count":
        df = df.groupby(df["name"]).count()
        print(df)
        ytitle = "count"
        yfield = "price"
    elif args.target == "price":
        df = df.groupby(df["name"]).agg({"price": "sum"})
        print(df)
        ytitle = "sum(price)"
        yfield = "price"

    fig = px.bar(df, y=yfield)
    fig.update_xaxes(title=xtitle)
    fig.update_yaxes(title=ytitle)
    po.write_html(fig, file="output.html", auto_open=True)