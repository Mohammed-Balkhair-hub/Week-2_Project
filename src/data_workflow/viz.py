import pandas as pd
from pathlib import Path
import plotly.express as plty



#plotly chart reference : https://plotly.com/python/

def bar_sorted(df:pd.DataFrame, x: str, y:str, title:str):
   
    df = df.sort_values(ascending=False).reset_index()  # sort by y value becuase it is a bar chart
    fig = plty.bar(df, x=x, y=y, title=title)

    # update layout and axes titles
    fig.update_layout(title={"text": title, "x": 0.02})
    fig.update_xaxes(title_text=x)
    fig.update_yaxes(title_text=y)
    return fig



def time_line(df:pd.DataFrame, x:str, y:str, title:str):

    fig=plty.line(df,x=x,y=y,title=title)
    # update layout and axes titles
    fig.update_layout(title=title)
    fig.update_xaxes(title=x)
    fig.update_yaxes(title=y)
    return fig




def histogram_chart(df:pd.DataFrame, x:str, nbins, title:str):

    fig=plty.histogram(df, x=x,nbins=nbins)
    # update layout and axes titles
    fig.update_layout(title=title)
    fig.update_xaxes(title=x)
    return fig

def save_fig(fig, path, scale=2) -> None: # should be in io ??
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(path), scale=scale)