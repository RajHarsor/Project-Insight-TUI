from textual.app import App, ComposeResult, RenderResult
from textual.screen import Screen
from textual.widgets import Footer, Header, Button, Static
from textual.theme import Theme
from textual.containers import VerticalScroll, HorizontalGroup
from elements.menu_screen import MenuScreen  # Import the MenuScreen class from menu_screen.py

class MainGUI(App):
    TITLE = "Project Insight SMS GUI"

    def on_mount(self) -> None:
        self.theme = 'nord'
        self.install_screen(MenuScreen(), name = "menu")
        self.push_screen("menu")


if __name__ == "__main__":
    app = MainGUI()
    app.run()