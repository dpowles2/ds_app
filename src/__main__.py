from dash import Dash, callback, Input, Output, html, dcc, page_registry, page_container
import dash_bootstrap_components as dbc

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = Dash(__name__, use_pages=True, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1('Data Science Apps'),
    html.Div([
        html.Div(
            dcc.Link(f"{page['name']}", href=page["relative_path"])
        ) for page in page_registry.values()
    ]),
    page_container
])

def main():
    app.run(debug=True)

if __name__ == "__main__":
    main()
